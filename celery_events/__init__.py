from celery_events.backends import Backend
from celery_events.events import Registry

registry = Registry()


def _get_backend(backend_class):
    if not issubclass(backend_class, Backend):
        raise TypeError('backend_class is not a subclass of Backend.')

    return backend_class(registry)


def sync_local_events(backend_class):
    backend = _get_backend(backend_class)
    if sync_local_events:
        backend.sync_local_events()


def sync_remote_events(backend_class):
    backend = _get_backend(backend_class)
    if sync_remote_events:
        backend.sync_remote_events()
