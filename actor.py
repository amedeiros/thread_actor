from threading import Thread, RLock, Event
from queue import Queue, Empty
from abc import abstractmethod

from typing import Any, Dict

GLOBAL_LOCK = RLock()


class ActorAddress:
    _addr: str

    def __init__(self, addr: str):
        self._addr = addr

    def __str__(self):
        return f"ActorAddr-{self._addr}"


class BaseMessage:
    pass


class ActorShutdownMessage(BaseMessage):
    pass


class SystemMessage(BaseMessage):
    _sender: ActorAddress
    _msg: Any

    def __init__(self, sender: ActorAddress, msg: Any):
        self._sender = sender
        self._msg = msg

    def msg(self):
        return self._msg

    def sender(self):
        return self._sender


class Actor(Thread):
    _inbox: Queue
    _stop_event: Event
    _addr: ActorAddress

    def __init__(self, addr, *args, **kwargs):
        super().__init__(name=self.__class__.__name__, target=self._run, *args, **kwargs)
        self._inbox = Queue()
        self._stop_event = Event()
        self._addr = addr
        self.after_init()

    @abstractmethod
    def receive_message(self, msg: Any, sender: ActorAddress):
        pass

    def after_init(self):
        pass

    def _run(self):
        while not self._stop_event.is_set():
            try:
                msg: SystemMessage = self._inbox.get_nowait()

                self.receive_message(msg.msg(), msg.sender())

                if isinstance(msg.msg(), ActorShutdownMessage):
                    self._stop_event.set()
            except Empty:
                pass

    def put_inbox(self, msg: SystemMessage):
        self._inbox.put_nowait(msg)

    def send(self, addr: ActorAddress, msg: Any):
        ActorSystem().tell(addr, msg, sender=self._addr)

    @staticmethod
    def create_actor(actor_class, global_name=None) -> ActorAddress:
        return ActorSystem().create_actor(actor_class, global_name)


class ActorSystem:
    addr: ActorAddress
    actors: Dict[ActorAddress, Actor]
    clone_count: Dict[str, int]
    _system_base: Any  # ActorSystem singleton

    def __init__(self):
        self.addr = ActorAddress("/ActorSys")
        self.clone_count = {}
        self.actors = {}

        # Single Actor System
        with GLOBAL_LOCK:
            if "_system_base" not in self.__class__.__dict__:
                setattr(ActorSystem, "_system_base", self)

        self.system_base = getattr(ActorSystem, "_system_base")

    def shutdown(self):
        for addr in self.system_base.actors.keys():
            self.tell(addr, ActorShutdownMessage())

    def create_actor(self, actor_class, global_name=None) -> ActorAddress:
        if global_name is None:
            name = actor_class.__name__
            if name in self.system_base.clone_count:
                self.system_base.clone_count[name] += 1
            else:
                self.system_base.clone_count[name] = 1
            count = self.system_base.clone_count[name]
            global_name = f"{name}.{count}"

        addr = ActorAddress(global_name)
        actor = actor_class(addr)
        actor.start()

        self.system_base.actors[addr] = actor

        return addr

    def tell(self, addr: ActorAddress, msg: Any, sender=None):
        if addr not in self.system_base.actors or not isinstance(addr, ActorAddress):
            raise RuntimeError(f"Invalid actor address {addr}")

        sender = (sender or self.system_base.addr)
        actor: Actor = self.system_base.actors[addr]
        system_msg = SystemMessage(sender=sender, msg=msg)
        actor.put_inbox(system_msg)
