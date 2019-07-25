from celery import current_app, signature


class BroadcastTask(current_app.Task):
    """Broadcasts an event by calling the registered tasks."""

    name = 'celery_events.tasks.broadcast_task'

    def run(self, **kwargs):
        from celery_events import registry

        app_name = kwargs.pop('app_name', None)
        event_name = kwargs.pop('event_name', None)

        if app_name and event_name:
            event = registry.event(app_name, event_name, raise_does_not_exist=True)
            for task in event.tasks:
                signature(task.name, kwargs=kwargs, queue=task.queue).delay()


current_app.tasks.register(BroadcastTask())
