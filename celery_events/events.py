import logging

from celery_events.tasks import broadcast

logger = logging.getLogger(__name__)


class AppModel:

    def __new__(cls):
        from celery_events import app

        if app is None:
            raise RuntimeError('Application is not initialized.')

        obj = super().__new__(cls)
        obj.app = app
        return obj


class EventModel(AppModel):

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        is_remote = kwargs.pop('is_remote', False)
        backend_obj = kwargs.pop('backend_obj', None)
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


class Event(EventModel):
    """Event."""

    def __init__(self, app_name, event_name, kwarg_keys=None, accept_any_kwarg_keys=False):
        super().__init__()
        self.app_name = app_name
        self.event_name = event_name
        self.kwarg_keys = kwarg_keys or []
        self.tasks = []
        self.accept_any_kwarg_keys = accept_any_kwarg_keys

    def __eq__(self, other):
        return (self.app_name, self.event_name) == (other.app_name, other.event_name)

    def __hash__(self):
        return hash((self.app_name, self.event_name))

    def __str__(self):
        return '<{0}-{1}>'.format(self.app_name, self.event_name)

    def _check_kwargs(self, kwargs):
        if not self.accept_any_kwarg_keys:
            for key in kwargs.keys():
                if key not in self.kwarg_keys:
                    raise ValueError('Event does not accept kwarg {0}.'.format(key))

        for key, value in kwargs.items():
            if value is not None and not isinstance(value, (str, float, int, bool, list, dict)):
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

        if now:
            broadcast(**run_task_kwargs)
        else:
            broadcast.apply_async(kwargs=run_task_kwargs, queue=self.get_broadcast_queue())

    def add_task(self, task):
        if task not in self.tasks:
            self.tasks.append(task)

        return task

    def add_task_name(self, name, queue=None):
        return self._get_or_create_task(name, queue)

    def add_c_task(self, c_task, queue=None):
        return self._get_or_create_task(c_task.name, queue)

    def get_broadcast_queue(self):
        return 'events_broadcast'


class Task(EventModel):
    """Task for an event."""

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
            return self.get_task_name_queue(name)
        else:
            return queue

    def get_task_name_queue(self, task_name):
        return None
