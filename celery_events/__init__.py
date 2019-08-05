from celery_events.app import App, AppContainer

app = None

app_container = AppContainer()


def create_app(backend_class=None, get_broadcast_queue=None, get_task_name_queue=None):
    """Convenience method to create app."""

    global app
    app = App(
        backend_class=backend_class,
        get_broadcast_queue=get_broadcast_queue,
        get_task_name_queue=get_task_name_queue
    )
    app_container.app = app

    return app_container.app
