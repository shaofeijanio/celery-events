import datetime

from unittest import mock, TestCase

from celery_events import create_app, app_container
from celery_events.app import App
from celery_events.backends import Backend
from celery_events.events import Event, Task, Registry
from celery_events.tasks import broadcast


class EventTestCase(TestCase):

    def test_add_task(self):
        event = Event('app', 'event')
        task = Task('task')
        event.add_task(task)

        self.assertEqual([task], event.tasks)

    def test_add_task_task_already_added(self):
        event = Event('app', 'event')
        task = event.add_task(Task('task'))
        event.add_task(Task('task'))

        self.assertEqual([task], event.tasks)

    def test_add_task_name(self):
        event = Event('app', 'event')
        event.add_task_name('task')

        self.assertEqual([Task('task')], event.tasks)

    def test_add_task_name_task_already_added(self):
        event = Event('app', 'event')
        task = event.add_task(Task('task'))
        event.add_task_name('task')

        self.assertEqual([task], event.tasks)

    def test_add_c_task(self):
        class CTask:
            pass

        c_task = CTask()
        c_task.name = 'task'
        event = Event('app', 'event')
        event.add_c_task(c_task)

        self.assertEqual([Task('task')], event.tasks)

    def test_add_c_task_task_already_added(self):
        class CTask:
            pass

        c_task = CTask()
        c_task.name = 'task'
        event = Event('app', 'event')
        task = event.add_task(Task('task'))
        event.add_c_task(c_task)

        self.assertEqual([task], event.tasks)

    @mock.patch('celery_events.events.broadcast')
    def test_broadcast(self, mock_broadcast_task):
        event = Event('app', 'event')
        event.broadcast()

        mock_broadcast_task.apply_async.assert_called_with(
            kwargs={'app_name': 'app', 'event_name': 'event'},
            queue='events_broadcast'
        )

    def test_broadcast_invalid_kwarg_key(self):
        event = Event('app', 'event')
        try:
            event.broadcast(a=1)
            self.fail()
        except ValueError:
            pass

        event = Event('app', 'event', kwarg_keys=['a'])
        try:
            event.broadcast(b=1)
            self.fail()
        except ValueError:
            pass

    def test_broadcast_invalid_kwarg_type(self):
        event = Event('app', 'event', kwarg_keys=['a'])
        try:
            event.broadcast(a={1, 2, 3})
            self.fail()
        except TypeError:
            pass

        try:
            event.broadcast(a=(1, 2, 3))
            self.fail()
        except TypeError:
            pass

        try:
            event.broadcast(a=datetime.datetime(2019, 1, 1))
            self.fail()
        except TypeError:
            pass

    def test_broadcast_remote_event(self):
        event = Event.remote_instance('app', 'event')
        try:
            event.broadcast()
            self.fail()
        except RuntimeError:
            pass

    @mock.patch('celery_events.events.broadcast')
    def test_broadcast_valid_kwargs(self, mock_broadcast_task):
        event = Event('app', 'event', kwarg_keys=['a', 'b'])

        event.broadcast(a=1)
        mock_broadcast_task.apply_async.assert_called_with(
            kwargs={'app_name': 'app', 'event_name': 'event', 'a': 1},
            queue='events_broadcast'
        )

        event.broadcast(a=1, b=2)
        mock_broadcast_task.apply_async.assert_called_with(
            kwargs={'app_name': 'app', 'event_name': 'event', 'a': 1, 'b': 2},
            queue='events_broadcast'
        )

    @mock.patch('celery_events.events.broadcast')
    def test_broadcast_now(self, mock_broadcast_task):
        event = Event('app', 'event')
        event.broadcast(now=True)

        mock_broadcast_task.assert_called_with(app_name='app', event_name='event')


class TaskTestCase(TestCase):

    def test_use_args_queue(self):
        task = Task('task', queue='queue')
        self.assertEqual('queue', task.queue)

    def test_no_override_get_name_queue(self):
        task = Task('task')
        self.assertIsNone(task.queue)

    def test_override_get_name_queue(self):
        class TestTask(Task):

            def get_task_name_queue(self, task_name):
                return 'queue'

        task = TestTask('task')
        self.assertEqual('queue', task.queue)


class RegistryTestCase(TestCase):

    def tearDown(self):
        Event.get_broadcast_queue = lambda _self: 'events_broadcast'
        Task.get_task_name_queue = lambda _self, task_name: None

    def test_set_get_broadcast_queue(self):
        registry = Registry()
        registry.set_get_broadcast_queue(lambda: 'new_queue')
        self.assertEqual('new_queue', Event('app', 'event').get_broadcast_queue())

    def test_set_get_task_name_queue(self):
        registry = Registry()
        registry.set_get_task_name_queue(lambda task_name: 'queue')
        self.assertEqual('queue', Task('task').get_task_name_queue('task'))

    def test_event(self):
        registry = Registry()
        existing_event = Event('app', 'event')
        registry.events.append(existing_event)
        event = registry.event('app', 'event')
        self.assertEqual(existing_event, event)

    def test_event_local_only(self):
        registry = Registry()
        local_event = Event.local_instance('app local', 'event')
        remote_event = Event.remote_instance('app remote', 'event')
        registry.events.append(local_event)
        registry.events.append(remote_event)
        event = registry.event('app remote', 'event', local_only=True)
        self.assertIsNone(event)

    def test_event_remote_only(self):
        registry = Registry()
        local_event = Event.local_instance('app local', 'event')
        remote_event = Event.remote_instance('app remote', 'event')
        registry.events.append(local_event)
        registry.events.append(remote_event)
        event = registry.event('app local', 'event', remote_only=True)
        self.assertIsNone(event)

    def test_event_no_event(self):
        registry = Registry()
        event = registry.event('app', 'event')
        self.assertIsNone(event)

    def test_event_no_event_raise_does_not_exist(self):
        registry = Registry()
        try:
            registry.event('app', 'event', raise_does_not_exist=True)
            self.fail()
        except RuntimeError:
            pass

    def test_create_local_event(self):
        registry = Registry()
        event = registry.create_local_event('app', 'event', kwarg_keys=['a', 'b'])
        self.assertEqual(['a', 'b'], event.kwarg_keys)
        self.assertEqual([event], registry.events)
        self.assertFalse(event.is_remote)

    def test_create_local_event_already_created(self):
        registry = Registry()
        registry.create_local_event('app', 'event')
        event = registry.create_local_event('app', 'event')
        self.assertEqual([], event.kwarg_keys)
        self.assertEqual([event], registry.events)
        self.assertFalse(event.is_remote)

    def test_local_event(self):
        registry = Registry()
        existing_event = Event.local_instance('app', 'event')
        registry.events.append(existing_event)
        event = registry.local_event('app', 'event')
        self.assertEqual(existing_event, event)
        self.assertFalse(event.is_remote)

    def test_local_event_no_event(self):
        registry = Registry()
        try:
            event = registry.local_event('app', 'event')
            self.fail()
        except RuntimeError:
            pass

    def test_local_event_only_remote_event(self):
        registry = Registry()
        existing_event = Event.remote_instance('app', 'event')
        registry.events.append(existing_event)
        try:
            event = registry.local_event('app', 'event')
            self.fail()
        except RuntimeError:
            pass

    def test_remote_event(self):
        registry = Registry()
        existing_event = Event.remote_instance('app', 'event')
        registry.events.append(existing_event)
        event = registry.remote_event('app', 'event')
        self.assertEqual(existing_event, event)
        self.assertTrue(event.is_remote)

    def test_remote_event_no_event(self):
        registry = Registry()
        event = registry.remote_event('app', 'event')
        expected_event = Event.remote_instance('app', 'event')
        self.assertEqual(expected_event, event)
        self.assertTrue(event.is_remote)

    def test_remote_event_only_local_event(self):
        registry = Registry()
        registry.create_local_event('app', 'event')
        event = registry.remote_event('app', 'event')
        expected_event = Event.remote_instance('app', 'event')
        self.assertEqual(expected_event, event)
        self.assertTrue(event.is_remote)

    def test_local_events(self):
        registry = Registry()
        local_event = Event.local_instance('app local', 'event')
        remote_event = Event.remote_instance('app remote', 'event')
        registry.events.append(local_event)
        registry.events.append(remote_event)

        self.assertEqual([local_event], registry.local_events)

    def test_remote_events(self):
        registry = Registry()
        local_event = Event.local_instance('app local', 'event')
        remote_event = Event.remote_instance('app remote', 'event')
        registry.events.append(local_event)
        registry.events.append(remote_event)

        self.assertEqual([remote_event], registry.remote_events)


class BackendTestCase(TestCase):

    def setUp(self):
        self.registry = Registry()
        self.namespaces = ['app_1', 'app_2']
        self.local_events_from_backend = []
        self.remote_events_from_backend = []
        self.deleted_events = []
        self.created_events = []
        self.created_tasks = []
        self.removed_tasks = []
        self.updated_tasks = []

        test_case = self

        class TestBackend(Backend):

            def get_local_namespaces(self):
                return test_case.namespaces

            def get_task_namespace(self, task):
                return task.name.split('.')[0]

            def fetch_events_for_namespaces(self, app_names):
                return test_case.local_events_from_backend

            def fetch_events(self, events):
                return [
                    event for event in test_case.local_events_from_backend + test_case.remote_events_from_backend
                    if event in events
                ]

            def should_update_event(self, event):
                return True

            def delete_events(self, events):
                for event in events:
                    test_case.deleted_events.append(event)

            def create_events(self, events):
                for event in events:
                    test_case.created_events.append(event)

            def create_tasks(self, event, tasks):
                for task in tasks:
                    test_case.created_tasks.append((event, task))

            def remove_tasks(self, event, tasks):
                for task in tasks:
                    test_case.removed_tasks.append((event, task))

            def update_tasks(self, tasks):
                for task in tasks:
                    test_case.updated_tasks.append(task)

        self.backend_cls = TestBackend

    def test_update_local_event(self):
        local_event = self.registry.create_local_event('app_1', 'event_1')
        local_event.add_task_name('task_1')
        local_event_from_backend = Event.local_instance('app_1', 'event_1')
        self.local_events_from_backend.append(local_event_from_backend)
        local_event_from_backend.add_task_name('task_2')

        backend = self.backend_cls(self.registry)
        backend.update_local_event(local_event)

        self.assertEqual(1, len(self.registry.events))
        self.assertEqual(local_event, self.registry.events[0])
        self.assertEqual([Task('task_1'), Task('task_2')], self.registry.events[0].tasks)

    def test_update_local_event_no_remote_tasks(self):
        local_event = self.registry.create_local_event('app_1', 'event_1')
        local_event.add_task_name('task_1')
        local_event_from_backend = Event.local_instance('app_1', 'event_1')
        self.local_events_from_backend.append(local_event_from_backend)

        backend = self.backend_cls(self.registry)
        backend.update_local_event(local_event)

        self.assertEqual(1, len(self.registry.events))
        self.assertEqual(local_event, self.registry.events[0])
        self.assertEqual([Task('task_1')], self.registry.events[0].tasks)

    def test_update_local_event_no_event_from_backend(self):
        local_event = self.registry.create_local_event('app_1', 'event_1')
        local_event.add_task_name('task_1')

        backend = self.backend_cls(self.registry)
        backend.update_local_event(local_event)

        self.assertEqual(1, len(self.registry.events))
        self.assertEqual(local_event, self.registry.events[0])
        self.assertEqual([Task('task_1')], self.registry.events[0].tasks)

    def test_sync_local_events_create_event(self):
        local_event = self.registry.create_local_event('app_1', 'event_1')
        local_event.add_task_name('task_1')

        backend = self.backend_cls(self.registry)
        backend.sync_local_events()

        self.assertEqual(1, len(self.created_events))
        created_event = self.created_events[0]
        self.assertEqual(local_event, created_event)
        self.assertEqual(0, len(self.created_tasks))
        self.assertEqual(0, len(self.deleted_events))
        self.assertEqual(0, len(self.removed_tasks))
        self.assertEqual(0, len(self.updated_tasks))

    def test_sync_local_events_delete_event(self):
        local_event = Event.local_instance('app_1', 'event_1')
        self.local_events_from_backend.append(local_event)
        local_event.add_task_name('task_1')

        backend = self.backend_cls(self.registry)
        backend.sync_local_events()

        self.assertEqual(0, len(self.created_events))
        self.assertEqual(0, len(self.created_tasks))
        self.assertEqual(1, len(self.deleted_events))
        deleted_event = self.deleted_events[0]
        self.assertEqual(local_event, deleted_event)
        self.assertEqual(0, len(self.removed_tasks))
        self.assertEqual(0, len(self.updated_tasks))

    def test_sync_remote_events_create_task(self):
        remote_event = self.registry.remote_event('app_3', 'event_3')
        self.remote_events_from_backend.append(remote_event)
        local_task = remote_event.add_task_name('task_1')

        backend = self.backend_cls(self.registry)
        backend.sync_remote_events()

        self.assertEqual(0, len(self.created_events))
        self.assertEqual(1, len(self.created_tasks))
        event, created_task = self.created_tasks[0]
        self.assertEqual(remote_event, event)
        self.assertEqual(local_task, created_task)
        self.assertEqual(0, len(self.deleted_events))
        self.assertEqual(0, len(self.removed_tasks))
        self.assertEqual(0, len(self.updated_tasks))

    def test_sync_remote_events_remove_task(self):
        remote_event = self.registry.remote_event('app_3', 'event_3')
        remote_event_from_backend = Event.remote_instance('app_3', 'event_3')
        self.remote_events_from_backend.append(remote_event_from_backend)
        local_task_from_backend = remote_event_from_backend.add_task_name('app_1.task_1')

        backend = self.backend_cls(self.registry)
        backend.sync_remote_events()

        self.assertEqual(0, len(self.created_events))
        self.assertEqual(0, len(self.created_tasks))
        self.assertEqual(0, len(self.deleted_events))
        self.assertEqual(1, len(self.removed_tasks))
        event, removed_task = self.removed_tasks[0]
        self.assertEqual(remote_event, event)
        self.assertEqual(local_task_from_backend, removed_task)
        self.assertEqual(0, len(self.updated_tasks))

    def test_sync_remove_events_update_task(self):
        remote_event = self.registry.remote_event('app_3', 'event_3')
        remote_event_from_backend = Event.remote_instance('app_3', 'event_3')
        self.remote_events_from_backend.append(remote_event_from_backend)
        local_task = remote_event.add_task_name('app_1.task_1', queue='new_queue')
        remote_event_from_backend.add_task_name('app_1.task_1', queue='old_queue')

        backend = self.backend_cls(self.registry)
        backend.sync_remote_events()

        self.assertEqual(0, len(self.created_events))
        self.assertEqual(0, len(self.created_tasks))
        self.assertEqual(0, len(self.deleted_events))
        self.assertEqual(0, len(self.removed_tasks))
        self.assertEqual(1, len(self.updated_tasks))
        updated_task = self.updated_tasks[0]
        self.assertEqual(local_task, updated_task)
        self.assertEqual(local_task.queue, updated_task.queue)

    def test_sync_remove_events_remote_event_not_found(self):
        self.registry.remote_event('app_3', 'event_3')

        backend = self.backend_cls(self.registry)
        backend.sync_remote_events()

        self.assertEqual(0, len(self.created_events))
        self.assertEqual(0, len(self.created_tasks))
        self.assertEqual(0, len(self.deleted_events))
        self.assertEqual(0, len(self.removed_tasks))
        self.assertEqual(0, len(self.updated_tasks))


class BroadcastTaskTestCase(TestCase):

    def setUp(self):
        self.update_local_event_events = []
        test_case = self

        class TestBacked(Backend):

            def update_local_event(self, event):
                test_case.update_local_event_events.append(event)

            def get_local_namespaces(self):
                return []

        self.app = create_app(TestBacked)

    @mock.patch('celery_events.tasks.signature')
    def test_run(self, mock_signature):
        event = self.app.registry.create_local_event('app', 'event', kwarg_keys=['a', 'b'])
        event.add_task_name('task', queue='queue')
        broadcast(app_name='app', event_name='event', a='a', b='b')
        self.assertEqual([event], self.update_local_event_events)
        mock_signature.assert_called_with('task', kwargs={'a': 'a', 'b': 'b'}, queue='queue')

    @mock.patch('celery_events.tasks.signature')
    def test_run_no_app_name_event_name(self, mock_signature):
        broadcast(a='a', b='b')
        mock_signature.assert_not_called()

    def test_run_no_event(self):
        try:
            broadcast(app_name='app', event_name='event', a='a', b='b')
            self.fail()
        except RuntimeError:
            pass

    def test_run_no_app(self):
        self.app.registry.create_local_event('app', 'event', kwarg_keys=['a', 'b'])
        app_container.app = None
        try:
            broadcast(app_name='app', event_name='event', a='a', b='b')
            self.fail()
        except RuntimeError:
            pass


class AppTestCase(TestCase):

    def tearDown(self):
        Event.get_broadcast_queue = lambda _self: 'events_broadcast'
        Task.get_task_name_queue = lambda _self, task_name: None

    def test_update_local_event(self):
        update_local_event_events = []

        class TestBackend(Backend):

            def update_local_event(self, event):
                update_local_event_events.append(event)

            def get_local_namespaces(self):
                return []

        event = Event('app_name', 'event_name')
        app = App(TestBackend)
        app.update_local_event(event)
        self.assertEqual([event], update_local_event_events)

    def test_sync_events(self):
        sync_local_events_called_times = []
        sync_remote_events_called_times = []

        class TestBackend(Backend):

            def sync_local_events(self):
                sync_local_events_called_times.append(1)

            def sync_remote_events(self):
                sync_remote_events_called_times.append(1)

            def get_local_namespaces(self):
                return []

        app = App(TestBackend)
        app.sync_local_events()
        self.assertEqual(1, len(sync_local_events_called_times))
        app.sync_remote_events()
        self.assertEqual(1, len(sync_remote_events_called_times))

    def test_invalid_backend_class(self):
        class InvalidBackend:
            pass

        try:
            App(InvalidBackend)
            self.fail()
        except TypeError:
            pass

    def test_set_registry(self):
        def get_broadcast_queue():
            return 'new_broadcast_queue'

        def get_task_name_queue(task_name):
            return 'queue_{0}'.format(task_name)

        App(Backend, get_broadcast_queue=get_broadcast_queue, get_task_name_queue=get_task_name_queue)

        broadcast_queue = Event('app', 'event').get_broadcast_queue()
        self.assertEqual('new_broadcast_queue', broadcast_queue)
        task_name_queue = Task('task').get_task_name_queue('task')
        self.assertEqual('queue_task', task_name_queue)
