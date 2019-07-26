from celery import shared_task, signature


@shared_task
def broadcast(**kwargs):
    """Broadcasts an event by calling the registered tasks."""

    from celery_events import app_container

    app = app_container.app

    if app is None:
        raise RuntimeError('App not found. Add initialised app to celery_events.app_container.app.')

    app_name = kwargs.pop('app_name', None)
    event_name = kwargs.pop('event_name', None)

    if app_name and event_name:
        app.update_local_events()
        event = app.registry.event(app_name, event_name, raise_does_not_exist=True)
        for task in event.tasks:
            signature(task.name, kwargs=kwargs, queue=task.queue).delay()
