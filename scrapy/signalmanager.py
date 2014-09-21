from scrapy.xlib.pydispatch import dispatcher
from scrapy.utils import signal

class SignalManager(object):
    """
    Code has big different with document description.
    Seems like now SignalManager is just a proxy of
    dispatcher, all the signal actions are been done
    inside dispatcher.
    """

    def __init__(self, sender=dispatcher.Anonymous):
        self.sender = sender

    def connect(self, *a, **kw):
        """
        connect a reciever to a signal.
        """
        kw.setdefault('sender', self.sender)
        return dispatcher.connect(*a, **kw)

    def disconnect(self, *a, **kw):
        """
        disconnect a reciever to a signal.
        """
        kw.setdefault('sender', self.sender)
        return dispatcher.disconnect(*a, **kw)

    def send_catch_log(self, *a, **kw):
        """
        send signal, catch exception and log them.
        """
        kw.setdefault('sender', self.sender)
        return signal.send_catch_log(*a, **kw)

    def send_catch_log_deferred(self, *a, **kw):
        """
        send signal async, and catch exception and log them.
        """
        kw.setdefault('sender', self.sender)
        return signal.send_catch_log_deferred(*a, **kw)

    def disconnect_all(self, *a, **kw):
        """
        disconnect all revievers from signals.
        """
        kw.setdefault('sender', self.sender)
        return signal.disconnect_all(*a, **kw)
