from celery_events.backends import Backend
from celery_events.events import Registry


class App:

    def __init__(self, backend_class, get_broadcast_queue=None, get_task_name_queue=None):
        if not issubclass(backend_class, Backend):
            raise TypeError('backend_class is not a subclass of Backend.')

        self.backend_class = backend_class
        self.registry = Registry()

        if get_broadcast_queue is not None:
            self.registry.set_get_broadcast_queue(get_broadcast_queue)
        if get_task_name_queue is not None:
            self.registry.set_get_task_name_queue(get_task_name_queue)

    def _get_backend(self):
        return self.backend_class(self.registry)

    def update_local_events(self):
        backend = self._get_backend()
        backend.update_local_events()

    def sync_local_events(self):
        backend = self._get_backend()
        backend.sync_local_events()

    def sync_remote_events(self):
        backend = self._get_backend()
        backend.sync_remote_events()


class AppContainer:

    def __init__(self):
        self.app = None
