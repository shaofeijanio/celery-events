from celery_events.app import App, AppContainer

app_container = AppContainer()


def create_app(backend_class, get_broadcast_queue=None, get_task_name_queue=None):
    """Convenience method to create app."""

    app_container.app = App(
        backend_class,
        get_broadcast_queue=get_broadcast_queue,
        get_task_name_queue=get_task_name_queue
    )

    return app_container.app
