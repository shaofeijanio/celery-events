import logging

from celery import shared_task, signature

logger = logging.getLogger(__name__)


class Broadcaster:

    def __init__(self, app, queue, broadcast_task_base=None):

        def _broadcast(**kwargs):
            """Broadcasts an event by calling the registered tasks."""

            app_name = kwargs.pop('app_name', None)
            event_name = kwargs.pop('event_name', None)

            if app_name and event_name:
                event = app.registry.event(app_name, event_name, raise_does_not_exist=True)
                app.update_local_event(event)
                for task in event.tasks:
                    signature(task.name, kwargs=kwargs, queue=task.queue).delay()

        self.queue = queue
        self.broadcast_func = _broadcast
        self.broadcast_task = shared_task(base=broadcast_task_base)(_broadcast)

    def broadcast_async(self, **kwargs):
        self.broadcast_task.apply_async(kwargs=kwargs, queue=self.queue)

    def broadcast_sync(self, **kwargs):
        self.broadcast_func(**kwargs)


class EventModel:

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        app, is_remote, is_backend, backend_obj = cls._pop_instance_kwargs(kwargs)
        obj.app = app
        obj.is_remote = is_remote
        obj.is_backend = is_backend
        obj.backend_obj = backend_obj
        return obj

    @classmethod
    def _pop_instance_kwargs(cls, kwargs):
        app = kwargs.pop('app', None)
        is_remote = kwargs.pop('is_remote', False)
        is_backend = kwargs.pop('is_backend', False)
        backend_obj = kwargs.pop('backend_obj', None)
        return app, is_remote, is_backend, backend_obj

    @classmethod
    def local_instance(cls, *args, **kwargs):
        kwargs['is_remote'] = False
        instance = cls.__new__(cls, *args, **kwargs)
        cls._pop_instance_kwargs(kwargs)
        instance.__init__(*args, **kwargs)
        return instance

    @classmethod
    def remote_instance(cls, *args, **kwargs):
        kwargs['is_remote'] = True
        instance = cls.__new__(cls, *args, **kwargs)
        cls._pop_instance_kwargs(kwargs)
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

    def _get_or_create_task(self, name, queue, is_remote):
        task = next((t for t in self.tasks if t.name == name), None)
        if task is None:
            if is_remote:
                task = Task.remote_instance(name=name, queue=queue, app=self.app)
            else:
                task = Task.local_instance(name=name, queue=queue, app=self.app)

            self.tasks.append(task)

        return task

    def broadcast(self, now=False, **kwargs):
        if self.is_remote or self.is_backend:
            raise RuntimeError('Cannot broadcast a remote or backend event.')

        self._check_kwargs(kwargs)
        broadcast_kwargs = {
            'app_name': self.app_name,
            'event_name': self.event_name,
            **kwargs
        }

        if now:
            self.app.broadcaster.broadcast_sync(**broadcast_kwargs)
        else:
            self.app.broadcaster.broadcast_async(**broadcast_kwargs)

    def add_task(self, task):
        if task not in self.tasks:
            self.tasks.append(task)

        return task

    def add_remote_task_name(self, name, queue=None):
        return self._get_or_create_task(name, queue, True)

    def add_local_c_task(self, c_task, queue=None):
        return self._get_or_create_task(c_task.name, queue, False)


class Task(EventModel):
    """Task for an event."""

    def __init__(self, name, queue=None):
        super().__init__()
        self.name = name
        if self.is_remote:
            self.queue = queue
        else:
            self.queue = queue or self.app.route(name)

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name
