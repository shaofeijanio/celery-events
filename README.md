## Installation

```shell script
pip install git+https://github.com/shaofeijanio/celery-events.git@master#egg=celery-events
```

Note that [Celery](http://www.celeryproject.org/) is a dependency and should be installed separately.

## Initialization

Initialize a celery-events application with the below code. This code can reside in the `__init__.py` of your module.

```python
from celery_events import App
from celery_events.backends import Backend


class MyBackend(Backend):
    # Implement backend class
    ...


def get_broadcast_queue():
    # Return queue name for broadcast task
    ...


def get_task_name_queue(task_name):
    # Return queue name for task_name
    ...


app = App(
    backend_class=MyBackend,
    get_broadcast_queue=get_broadcast_queue,
    get_task_name_queue=get_task_name_queue
)
```

## Managing events

Events can be managed using the registry from the app object created by the `create_app()` method.

```python
# Some celery tasks to handle event
@shared_task
def handle_event_by_doing_a(arg_1, arg_2):
    print(arg_1, arg_2)

@shared_task
def handle_event_by_doing_b(arg_1, arg_2):
    print(arg_2, arg_1)


# Create a event using the app object created by create_app()
EVENT = app.registry.create_local_event('app_name', 'event_name', kwarg_keys=['arg_1', 'arg_2'])


# Add tasks to event
EVENT.add_c_task(handle_event_by_doing_a)
EVENT.add_c_task(handle_event_by_doing_b)


# Broadcast a event to trigger all tasks added to event
EVENT.broadcast(arg_1='a', arg_2='b')
```

The `broadcast()` method of a event will run all the tasks that will be added to the event. The above code will result 
in the below output.

```shell script
a, b
b, a
```

## Cross-application support

Multiple applications running the celery-events framework are able to access each others' events and add tasks to events
across applications. A remote event can be declared in the follow way. Adding tasks and triggering the events are the
same as before.

```python
# Declare a remote event using the app object created by create_app()
REMOTE_EVENT = app.registry.remote_event('remote_app_name', 'remote_event_name')
```

## Backends

If cross-application event is required, there needs to be a way for remote applications to know about the tasks that are
registered to them. This requires a `Backend` class to be implemented, which is used to publish local tasks to remote
events and get the remote tasks that are registered with local events. More information on how to override a backend 
class can be found in `celery_events.backends.Backend`. 

To specify the backend class to use, pass it as the `backend_class` argument in the `create_app()` method. If
 `backend_class` is not specified, no synchronization will be done with the backend and the application can only process
local events.
