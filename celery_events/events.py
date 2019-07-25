import logging

from celery_events import tasks

logger = logging.getLogger(__name__)


class BackendModel:

    def __new__(cls, *args, **kwargs):
        is_remote = kwargs.pop('is_remote', False)
        backend_obj = kwargs.pop('backend_obj', None)
        obj = super().__new__(cls)
        obj.is_remote = is_remote
        obj.backend_obj = backend_obj
        return obj

    @classmethod
    def local_instance(cls, *args, **kwargs):
        instance = cls.__new__(cls, is_remote=False)
        instance.__init__(*args, **kwargs)
        return instance

    @classmethod
    def remote_instance(cls, *args, **kwargs):
        instance = cls.__new__(cls, is_remote=True)
        instance.__init__(*args, **kwargs)
        return instance


class Event(BackendModel):
    """Event."""

    broadcast_task_queue = 'events_broadcast'

    def __init__(self, app_name, event_name, kwarg_keys=None):
        super().__init__()
        self.app_name = app_name
        self.event_name = event_name
        self.kwarg_keys = kwarg_keys or []
        self.tasks = []

    def __eq__(self, other):
        return (self.app_name, self.event_name) == (other.app_name, other.event_name)

    def __hash__(self):
        return hash((self.app_name, self.event_name))

    def __str__(self):
        return '<{0}-{1}>'.format(self.app_name, self.event_name)

    def _check_kwargs(self, kwargs):
        for key in kwargs.keys():
            if key not in self.kwarg_keys:
                raise ValueError('Event does not accept kwarg {0}.'.format(key))

        for key, value in kwargs.items():
            if not isinstance(value, (str, float, int, bool, list, dict)):
                raise TypeError('Kwarg {0} is not a valid JSON serializable type.'.format(key))

    def _get_or_create_task(self, name, queue):
        task = next((t for t in self.tasks if t.name == name), None)
        if task is None:
            task = Task.local_instance(name=name, queue=queue)
            self.tasks.append(task)

        return task

    def broadcast(self, now=False, **kwargs):
        if self.is_remote:
            raise RuntimeError('Cannot broadcast a remote event.')

        self._check_kwargs(kwargs)
        run_task_kwargs = {
            'app_name': self.app_name,
            'event_name': self.event_name,
            **kwargs
        }
        broadcast_task = tasks.BroadcastTask()

        if now:
            broadcast_task.run(**run_task_kwargs)
        else:
            broadcast_task.apply_async(kwargs=run_task_kwargs, queue=self.broadcast_task_queue)

    def add_task(self, task):
        if task not in self.tasks:
            self.tasks.append(task)

        return task

    def add_task_name(self, name, queue=None):
        return self._get_or_create_task(name, queue)

    def add_c_task(self, c_task, queue=None):
        return self._get_or_create_task(c_task.name, queue)


class Task(BackendModel):
    """Task for an event."""

    task_name_queue = {}

    def __init__(self, name, queue=None):
        super().__init__()
        self.name = name
        self.queue = self._get_queue(name, queue)

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def _get_queue(self, name, queue):
        if queue is None:
            return self.task_name_queue.get(name)
        else:
            return queue


class Registry:

    def __init__(self):
        self.events = []

    # Configuration methods #
    def set_broadcast_task_queue(self, queue):
        Event.broadcast_task_queue = queue

    def set_task_name_queue(self, task_name_queue):
        Task.task_name_queue = task_name_queue

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

    def create_local_event(self, app_name, event_name, kwarg_keys=None):
        event = self.event(app_name, event_name, local_only=True, raise_does_not_exist=False)
        if event is None:
            event = Event.local_instance(app_name=app_name, event_name=event_name, kwarg_keys=kwarg_keys)
            self.events.append(event)

        return event

    def local_event(self, app_name, event_name):
        return self.event(app_name, event_name, local_only=True, raise_does_not_exist=True)

    def remote_event(self, app_name, event_name):
        event = self.event(app_name, event_name, remote_only=True, raise_does_not_exist=False)
        if event is None:
            event = Event.remote_instance(app_name=app_name, event_name=event_name)
            self.events.append(event)

        return event
