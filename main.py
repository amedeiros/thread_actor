import actor
import time
from typing import Any, Optional
from actor import ActorShutdownMessage


class EchoMessage(actor.BaseMessage):
    msg: str

    def __init__(self, msg: str):
        self.msg = msg


class EchoResponse(actor.BaseMessage):
    msg: str

    def __init__(self, msg: str):
        self.msg = msg


class PrinterActor(actor.Actor):
    def receive_message(self, msg: Any, sender: actor.ActorAddress):
        if not isinstance(msg, ActorShutdownMessage):
            print({"msg": msg, "sender": f"{sender}"})
            self.send(sender, EchoResponse(f"Got your message! '{msg}'"))
        

class ProxyActor(actor.Actor):
    _other: Optional[actor.ActorAddress]

    def after_init(self):
        self._other = self.create_actor(PrinterActor)

    def receive_message(self, msg: Any, sender: actor.ActorAddress):
        if isinstance(msg, EchoMessage):
            self.send(self._other, msg.msg)
        elif isinstance(msg, EchoResponse):
            print(f"Response: {msg.msg} from {sender}")


if __name__ == '__main__':
    asy = actor.ActorSystem()
    actor = asy.create_actor(ProxyActor)
    asy.tell(actor, EchoMessage("Pass thru?"))
    time.sleep(2)
    asy.shutdown()
