import os
import json
from os.path import join, exists

from queuelib import PriorityQueue
from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.misc import load_object
from scrapy.utils.job import job_dir
from scrapy import log

class Scheduler(object):
    """
    The Scheduler receives requests from the engine and enqueues them
    for feeding them later (also to the engine) when the engine requests them.

    To sum up, scheduler is just a PriorityQueue that hold requests.
    """

    def __init__(self, dupefilter, jobdir=None, dqclass=None, mqclass=None, logunser=False, stats=None):
        # what is a dupe filter? it's a duplicated filter, just to
        # filter duplicated requests. It's a in memory filter.
        # Location: scrapy.dupefilter.RFPDupeFilter
        self.df = dupefilter
        # file dir of disk queue.
        self.dqdir = self._dqdir(jobdir)
        # as following, there are two kinds of queues in scheduler, disk
        # queue and memory queue, but don't know when to use them yet.
        # disk queue, scrapy.squeue.PickleLifoDiskQueue
        self.dqclass = dqclass
        # memory queue, scrapy.squeue.LifoMemoryQueue
        self.mqclass = mqclass
        # log unserializable requests. but if it's unseriablizable, how to
        # log it?
        self.logunser = logunser
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        """
        initialize scheduler instance with crawler, mainly for reading settings
        from crawler.
        """
        settings = crawler.settings
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])
        dupefilter = dupefilter_cls.from_settings(settings)
        dqclass = load_object(settings['SCHEDULER_DISK_QUEUE'])
        mqclass = load_object(settings['SCHEDULER_MEMORY_QUEUE'])
        logunser = settings.getbool('LOG_UNSERIALIZABLE_REQUESTS')
        return cls(dupefilter, job_dir(settings), dqclass, mqclass, logunser, crawler.stats)

    def has_pending_requests(self):
        # TODO len(self)?
        # see __len__
        return len(self) > 0

    def open(self, spider):
        """
        In this open method, just initialize two queues, and open
        filter(but do nothing)
        """
        self.spider = spider
        # what is mqs and dqs?
        # mqs is memory queue, dqs is disk queue.
        # initialize memory queue, it's a priority queue.
        # TODO PriorityQueue(self._newmq) this is interesting
        # memory queue been used when the request isn't serializable.
        self.mqs = PriorityQueue(self._newmq)
        self.dqs = self._dq() if self.dqdir else None
        # dupefilter.open do nothing
        return self.df.open()

    def close(self, reason):
        """
        close this scheduler, need to dump pending requests.
        """
        if self.dqs:
            prios = self.dqs.close()
            with open(join(self.dqdir, 'active.json'), 'w') as f:
                json.dump(prios, f)
        return self.df.close(reason)

    def enqueue_request(self, request):
        """
        push a request into queue for later processing. First try to
        push request into disk queue, then try to push into memory
        queue.
        """
        if not request.dont_filter and self.df.request_seen(request):
            # drop duplicated requests
            self.df.log(request, self.spider)
            return
        # when enqueue request, first try push into disk queue, if failed,
        # then try to push into memory queue.
        dqok = self._dqpush(request)
        if dqok:
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            self._mqpush(request)
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        # these kind of things just for statistic
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)

    def next_request(self):
        """
        get request from queue. mainly called by engine to get next request
        """
        request = self.mqs.pop()
        if request:
            self.stats.inc_value('scheduler/dequeued/memory', spider=self.spider)
        else:
            request = self._dqpop()
            if request:
                self.stats.inc_value('scheduler/dequeued/disk', spider=self.spider)
        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)
        return request

    def __len__(self):
        """
        get request length in this scheduler
        """
        return len(self.dqs) + len(self.mqs) if self.dqs else len(self.mqs)

    def _dqpush(self, request):
        """
        push request into disk queue(a PriorityQueue support serialization
        when stop)
        """
        if self.dqs is None:
            return
        try:
            reqd = request_to_dict(request, self.spider)
            self.dqs.push(reqd, -request.priority)
        except ValueError as e: # non serializable request
            if self.logunser:
                log.msg(format="Unable to serialize request: %(request)s - reason: %(reason)s",
                        level=log.ERROR, spider=self.spider,
                        request=request, reason=e)
            return
        else:
            return True

    def _mqpush(self, request):
        self.mqs.push(request, -request.priority)

    def _dqpop(self):
        if self.dqs:
            d = self.dqs.pop()
            if d:
                return request_from_dict(d, self.spider)

    def _newmq(self, priority):
        return self.mqclass()

    def _newdq(self, priority):
        return self.dqclass(join(self.dqdir, 'p%s' % priority))

    def _dq(self):
        """
        load disk queue requests into memory?
        """
        activef = join(self.dqdir, 'active.json')
        if exists(activef):
            with open(activef) as f:
                prios = json.load(f)
        else:
            prios = ()
        q = PriorityQueue(self._newdq, startprios=prios)
        if q:
            log.msg(format="Resuming crawl (%(queuesize)d requests scheduled)",
                    spider=self.spider, queuesize=len(q))
        return q

    def _dqdir(self, jobdir):
        if jobdir:
            dqdir = join(jobdir, 'requests.queue')
            if not exists(dqdir):
                os.makedirs(dqdir)
            return dqdir
