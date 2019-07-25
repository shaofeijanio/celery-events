from celery_events.backends import Backend
from celery_events.events import Registry

registry = Registry()


def _get_backend(backend_class):
    if not issubclass(backend_class, Backend):
        raise TypeError('backend_class is not a subclass of Backend.')

    return backend_class(registry)


def update_local_events(backend_class):
    backend = _get_backend(backend_class)
    backend.update_local_events()


def sync_local_events(backend_class):
    backend = _get_backend(backend_class)
    backend.sync_local_events()


def sync_remote_events(backend_class):
    backend = _get_backend(backend_class)
    backend.sync_remote_events()
