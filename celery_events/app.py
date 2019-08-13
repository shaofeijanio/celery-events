from celery_events.backends import Backend
from celery_events.events import Event, Broadcaster


class Registry:

    def __init__(self, app):
        self.app = app
        self.events = []

    # Event management methods #
    @property
    def local_events(self):
        return [event for event in self.events if not event.is_remote]

    @property
    def remote_events(self):
        return [event for event in self.events if event.is_remote]

    def event(self, app_name, event_name, local_only=False, remote_only=False, raise_does_not_exist=False):
        event = next(
            (
                event for event in self.events
                if
                (
                        (app_name, event_name) == (event.app_name, event.event_name) and
                        (not local_only or not event.is_remote) and
                        (not remote_only or event.is_remote)
                )
            ),
            None
        )
        if event is None and raise_does_not_exist:
            raise RuntimeError(
                'Event does not exist. If this event is a local event, it needs to be added via create_local_event().'
            )

        return event

    def create_local_event(self, app_name, event_name, kwarg_keys=None, accept_any_kwarg_keys=False):
        event = self.event(app_name, event_name, local_only=True, raise_does_not_exist=False)
        if event is None:
            event = Event.local_instance(
                app_name=app_name,
                event_name=event_name,
                kwarg_keys=kwarg_keys,
                accept_any_kwarg_keys=accept_any_kwarg_keys,
                app=self.app
            )
            self.events.append(event)

        return event

    def local_event(self, app_name, event_name):
        return self.event(app_name, event_name, local_only=True, raise_does_not_exist=True)

    def remote_event(self, app_name, event_name):
        event = self.event(app_name, event_name, remote_only=True, raise_does_not_exist=False)
        if event is None:
            event = Event.remote_instance(app_name=app_name, event_name=event_name, app=self.app)
            self.events.append(event)

        return event


class App:

    def __init__(self, backend_class=None, broadcast_queue=None, broadcast_task_base=None, routes=tuple()):
        if backend_class and not issubclass(backend_class, Backend):
            raise TypeError('backend_class is not a subclass of Backend.')

        self.backend_class = backend_class
        self.broadcaster = Broadcaster(
            self,
            broadcast_queue or 'events_broadcast',
            broadcast_task_base=broadcast_task_base
        )
        self.routes = routes
        self.registry = Registry(self)

    def _get_backend(self):
        return self.backend_class(self.registry)

    def update_local_event(self, event):
        if self.backend_class:
            backend = self._get_backend()
            backend.update_local_event(event)

    def sync_local_events(self):
        if self.backend_class:
            backend = self._get_backend()
            backend.sync_local_events()

    def sync_remote_events(self):
        if self.backend_class:
            backend = self._get_backend()
            backend.sync_remote_events()

    def route(self, task_name):
        for route in self.routes:

            class DummyTask:
                def __init__(self, name):
                    self.name = name

            r = route(name=task_name, args=None, kwargs=None, options=None, task=DummyTask(task_name))
            if isinstance(r, dict) and 'queue' in r:
                return r.get('queue')

        return None
