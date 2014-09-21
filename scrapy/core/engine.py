"""
This is the Scrapy engine which controls the Scheduler, Downloader and Spiders.

For more information see docs/topics/architecture.rst

The engine is responsible for controlling the data flow between all components
of the system, and triggering events when certain actions occur. See the Data
Flow section below for more details.
"""
import warnings
from time import time

from twisted.internet import defer
from twisted.python.failure import Failure

from scrapy import log, signals
from scrapy.core.downloader import Downloader
from scrapy.core.scraper import Scraper
from scrapy.exceptions import DontCloseSpider, ScrapyDeprecationWarning
from scrapy.http import Response, Request
from scrapy.utils.misc import load_object
from scrapy.utils.reactor import CallLaterOnce


class Slot(object):
    """
    what is this for? it's like a request queue, need to know when to use it.
    """
    def __init__(self, start_requests, close_if_idle, nextcall, scheduler):
        self.closing = False
        self.inprogress = set() # requests in progress
        self.start_requests = iter(start_requests)
        self.close_if_idle = close_if_idle
        self.nextcall = nextcall
        self.scheduler = scheduler

    def add_request(self, request):
        self.inprogress.add(request)

    def remove_request(self, request):
        self.inprogress.remove(request)
        self._maybe_fire_closing()

    def close(self):
        self.closing = defer.Deferred()
        self._maybe_fire_closing()
        return self.closing

    def _maybe_fire_closing(self):
        if self.closing and not self.inprogress:
            if self.nextcall:
                self.nextcall.cancel()
            self.closing.callback(None)


class ExecutionEngine(object):
    """
    This is the main engine to control data flow and actions callback.
    Some of definations not quite clear now, need to pay attention later.
    """

    def __init__(self, crawler, spider_closed_callback):
        # TODO crawler ralated information, need to understand later.
        # crawler is the instance of Crawler related with this engine.
        self.crawler = crawler
        # main settings, contains lots of definations and configurations.
        self.settings = crawler.settings
        # use signals(actually a SignalManager instance) to send all kinds
        # of signals to interested receivers.
        self.signals = crawler.signals
        # formatter of logging. Need to pay attention to.
        self.logformatter = crawler.logformatter
        # TODO maybe schedule related?
        # slot is a hard-to-describe concept, reserve for later.
        self.slot = None
        # related spider for this engine. Normally just one spider related
        # with one engine, one crawler.
        self.spider = None
        # indicating the running status of engine, for controlling engine
        # status and switch status.
        self.running = False
        # same with running
        self.paused = False
        # settings defined in crawler, and scheduler, downloader are configured
        # in settings in crawler. So the engine is constructed with crawler.
        # not like I think.
        self.scheduler_cls = load_object(self.settings['SCHEDULER'])
        downloader_cls = load_object(self.settings['DOWNLOADER'])
        self.downloader = downloader_cls(crawler)
        # TODO what is scraper for?
        # scraper is a concept more like extractor or linkcrawler, it's
        # for extracting useful information from downloaded web pages.
        self.scraper = Scraper(crawler)
        # TODO a concurrent spider? what is this for?
        self._concurrent_spiders = self.settings.getint('CONCURRENT_SPIDERS', 1)
        if self._concurrent_spiders != 1:
            warnings.warn("CONCURRENT_SPIDERS settings is deprecated, use " \
                "Scrapyd max_proc config instead", ScrapyDeprecationWarning)

        # TODO spider closed callback? like a error callback?
        self._spider_closed_callback = spider_closed_callback

    # NOTICE pay attention to inlineCallbacks, this decorator convert a normal
    # generator into in-ordered async callbacks. This is like a simulated callback
    # chain.
    @defer.inlineCallbacks
    def start(self):
        """Start the execution engine.

        Not much things been done in start, just send signals to other part, and
        set a variable to indicating the engine is running.
        """
        assert not self.running, "Engine already running"
        # record a start time, must used in logging.
        self.start_time = time()
        # before starting, first send engine started signal to all listeners(crawlers, maybe?).
        # use generator inside inlineCallbacks make this sync action, that means, generator
        # function will resume after all signals been sent.
        yield self.signals.send_catch_log_deferred(signal=signals.engine_started)
        # engine_started signals been sent, set variable value.
        self.running = True
        # set an empty callback to pause the start function. This function
        # is a generator and decorated with inlineCallbacks. Here because this
        # deferred will only be fired in stop() method, so will pause here, until
        # engine been stopped.
        # TODO we got twited reactor, why still do this way?
        self._closewait = defer.Deferred()
        yield self._closewait

    def stop(self):
        """Stop the execution engine gracefully"""
        assert self.running, "Engine not running"
        # why don't set to false after close all spiders?
        self.running = False
        # async close all spiders
        dfd = self._close_all_spiders()
        return dfd.addBoth(lambda _: self._finish_stopping_engine())

    def pause(self):
        """Pause the execution engine"""
        self.paused = True

    def unpause(self):
        """Resume the execution engine"""
        self.paused = False

    def _next_request(self, spider):
        slot = self.slot
        if not slot:
            return

        if self.paused:
            # run again 5 seconds later
            slot.nextcall.schedule(5)
            return

        while not self._needs_backout(spider):
            if not self._next_request_from_scheduler(spider):
                break

        if slot.start_requests and not self._needs_backout(spider):
            try:
                request = next(slot.start_requests)
            except StopIteration:
                slot.start_requests = None
            except Exception as exc:
                slot.start_requests = None
                log.err(None, 'Obtaining request from start requests', \
                        spider=spider)
            else:
                self.crawl(request, spider)

        if self.spider_is_idle(spider) and slot.close_if_idle:
            self._spider_idle(spider)

    def _needs_backout(self, spider):
        # TODO what is backout?
        slot = self.slot
        return not self.running \
            or slot.closing \
            or self.downloader.needs_backout() \
            or self.scraper.slot.needs_backout()

    def _next_request_from_scheduler(self, spider):
        slot = self.slot
        request = slot.scheduler.next_request()
        if not request:
            return
        d = self._download(request, spider)
        d.addBoth(self._handle_downloader_output, request, spider)
        d.addErrback(log.msg, spider=spider)
        d.addBoth(lambda _: slot.remove_request(request))
        d.addErrback(log.msg, spider=spider)
        d.addBoth(lambda _: slot.nextcall.schedule())
        d.addErrback(log.msg, spider=spider)
        return d

    def _handle_downloader_output(self, response, request, spider):
        assert isinstance(response, (Request, Response, Failure)), response
        # downloader middleware can return requests (for example, redirects)
        if isinstance(response, Request):
            self.crawl(response, spider)
            return
        # response is a Response or Failure
        d = self.scraper.enqueue_scrape(response, request, spider)
        d.addErrback(log.err, spider=spider)
        return d

    def spider_is_idle(self, spider):
        scraper_idle = self.scraper.slot.is_idle()
        pending = self.slot.scheduler.has_pending_requests()
        downloading = bool(self.downloader.active)
        pending_start_requests = self.slot.start_requests is not None
        idle = scraper_idle and not (pending or downloading or pending_start_requests)
        return idle

    @property
    def open_spiders(self):
        return [self.spider] if self.spider else []

    def has_capacity(self):
        """Does the engine have capacity to handle more spiders"""
        return not bool(self.slot)

    def crawl(self, request, spider):
        """
        start to schedule crawling request, add into schedule queue.
        """
        assert spider in self.open_spiders, \
            "Spider %r not opened when crawling: %s" % (spider.name, request)
        self.schedule(request, spider)
        self.slot.nextcall.schedule()

    def schedule(self, request, spider):
        """
        add the passing request into schedule queue
        """
        self.signals.send_catch_log(signal=signals.request_scheduled,
                request=request, spider=spider)
        return self.slot.scheduler.enqueue_request(request)

    def download(self, request, spider):
        slot = self.slot
        slot.add_request(request)
        d = self._download(request, spider)
        d.addBoth(self._downloaded, slot, request, spider)
        return d

    def _downloaded(self, response, slot, request, spider):
        # after downloading, remove request from slot
        slot.remove_request(request)
        # maybe a redirect request, add into slot to process request later.
        return self.download(response, spider) \
                if isinstance(response, Request) else response

    def _download(self, request, spider):
        slot = self.slot
        slot.add_request(request)
        def _on_success(response):
            assert isinstance(response, (Response, Request))
            if isinstance(response, Response):
                response.request = request # tie request to response received
                logkws = self.logformatter.crawled(request, response, spider)
                log.msg(spider=spider, **logkws)
                self.signals.send_catch_log(signal=signals.response_received, \
                    response=response, request=request, spider=spider)
            return response

        def _on_complete(_):
            slot.nextcall.schedule()
            return _

        dwld = self.downloader.fetch(request, spider)
        dwld.addCallbacks(_on_success)
        dwld.addBoth(_on_complete)
        return dwld

    @defer.inlineCallbacks
    def open_spider(self, spider, start_requests=(), close_if_idle=True):
        assert self.has_capacity(), "No free spider slot when opening %r" % \
            spider.name
        log.msg("Spider opened", spider=spider)
        # nextcall is a CallLaterOnce instance, the passing function will
        # be run in next reactor loop.
        nextcall = CallLaterOnce(self._next_request, spider)
        # initilize scheduler
        scheduler = self.scheduler_cls.from_crawler(self.crawler)
        # process requests
        start_requests = yield self.scraper.spidermw.process_start_requests(start_requests, spider)
        slot = Slot(start_requests, close_if_idle, nextcall, scheduler)
        self.slot = slot
        self.spider = spider
        # why yield this? open never returns deferred object. I guess it's just
        # to keep same with others, or for backup compatible, it's no hurt to
        # yield a non-deferred object.
        # in scheduler.open, just initialize queues for later usage.
        yield scheduler.open(spider)
        # open_spider in scraper allocate a slot for spider and then fire
        # execution of all open_spider method in middlewares and then return
        # a DeferredList.
        yield self.scraper.open_spider(spider)
        # open_spider in statscol do nothing.
        self.crawler.stats.open_spider(spider)
        # send spider opened signal.
        yield self.signals.send_catch_log_deferred(signals.spider_opened, spider=spider)
        # start schedule to run, will run in next reactor loop
        slot.nextcall.schedule()

    def _spider_idle(self, spider):
        """Called when a spider gets idle. This function is called when there
        are no remaining pages to download or schedule. It can be called
        multiple times. If some extension raises a DontCloseSpider exception
        (in the spider_idle signal handler) the spider is not closed until the
        next loop and this function is guaranteed to be called (at least) once
        again for this spider.
        """
        res = self.signals.send_catch_log(signal=signals.spider_idle, \
            spider=spider, dont_log=DontCloseSpider)
        if any(isinstance(x, Failure) and isinstance(x.value, DontCloseSpider) \
                for _, x in res):
            self.slot.nextcall.schedule(5)
            return

        if self.spider_is_idle(spider):
            self.close_spider(spider, reason='finished')

    def close_spider(self, spider, reason='cancelled'):
        """Close (cancel) spider and clear all its outstanding requests"""

        slot = self.slot
        if slot.closing:
            return slot.closing
        log.msg(format="Closing spider (%(reason)s)", reason=reason, spider=spider)

        dfd = slot.close()

        dfd.addBoth(lambda _: self.downloader.close())
        dfd.addErrback(log.err, spider=spider)

        dfd.addBoth(lambda _: self.scraper.close_spider(spider))
        dfd.addErrback(log.err, spider=spider)

        dfd.addBoth(lambda _: slot.scheduler.close(reason))
        dfd.addErrback(log.err, spider=spider)

        # XXX: spider_stats argument was added for backwards compatibility with
        # stats collection refactoring added in 0.15. it should be removed in 0.17.
        dfd.addBoth(lambda _: self.signals.send_catch_log_deferred(signal=signals.spider_closed, \
            spider=spider, reason=reason, spider_stats=self.crawler.stats.get_stats()))
        dfd.addErrback(log.err, spider=spider)

        dfd.addBoth(lambda _: self.crawler.stats.close_spider(spider, reason=reason))
        dfd.addErrback(log.err, spider=spider)

        dfd.addBoth(lambda _: log.msg(format="Spider closed (%(reason)s)", reason=reason, spider=spider))

        dfd.addBoth(lambda _: setattr(self, 'slot', None))
        dfd.addErrback(log.err, spider=spider)

        dfd.addBoth(lambda _: setattr(self, 'spider', None))
        dfd.addErrback(log.err, spider=spider)

        dfd.addBoth(lambda _: self._spider_closed_callback(spider))

        return dfd

    def _close_all_spiders(self):
        dfds = [self.close_spider(s, reason='shutdown') for s in self.open_spiders]
        dlist = defer.DeferredList(dfds)
        return dlist

    @defer.inlineCallbacks
    def _finish_stopping_engine(self):
        yield self.signals.send_catch_log_deferred(signal=signals.engine_stopped)
        self._closewait.callback(None)
