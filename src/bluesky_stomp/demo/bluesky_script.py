# Run with ipython -i src/bluesky_stomp/demo.py

import bluesky.plan_stubs as bps  # noqa F401
import bluesky.plans as bp  # noqa F401
import bluesky.preprocessors as bpp  # noqa F401
from bluesky import RunEngine
from ophyd_async.core import DeviceCollector
from ophyd_async.sim.demo import SimMotor

from bluesky_stomp.messaging.stomptemplate import Queue
from bluesky_stomp.producer import StompDocumentProducer, StompWaitingHook

# Set up RunEngine
RE = RunEngine()

RE.subscribe(StompDocumentProducer(Queue(name="documents")))
RE.waiting_hook = StompWaitingHook(Queue(name="status"))

# Set up devices
with DeviceCollector():
    x = SimMotor()
    y = SimMotor()

# Run a scan
RE(bp.scan([], x, 0.0, 10.0, y, 0.0, 10.0, 3))
