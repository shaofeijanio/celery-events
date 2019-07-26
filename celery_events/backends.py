class Backend:

    def __init__(self, registry):
        self.registry = registry
        self.local_task_namespaces = set(self.get_local_namespaces())
        self.local_events = set(self.registry.local_events)
        self.remote_events = set(self.registry.remote_events)
        self.local_events_from_backend = None
        self.remote_events_from_backend = None

    def _find_event(self, event, events):
        return next((e for e in events if e == event), None)

    def _find_task(self, task, tasks):
        return next((t for t in tasks if t == task), None)

    def _fetch_local_events(self):
        self.local_events_from_backend = set(self.fetch_events_for_namespaces(self.local_task_namespaces))
        self._set_backend_obj_for_events(self.local_events, self.local_events_from_backend)

    def _fetch_remote_events(self):
        self.remote_events_from_backend = set(self.fetch_events(self.remote_events))
        self._set_backend_obj_for_events(self.remote_events, self.remote_events_from_backend)

    def _set_backend_obj_for_events(self, events, events_from_backend):
        for event in events:
            event_from_backend = self._find_event(event, events_from_backend)
            if event_from_backend is not None:
                event.backend_obj = event_from_backend.backend_obj
                for task in event.tasks:
                    task_from_backend = self._find_task(task, event_from_backend.tasks)
                    if task_from_backend is not None:
                        task.backend_obj = task_from_backend.backend_obj

    # Public methods #
    def update_local_events(self):
        """
        Update local events in registry with tasks from remote.
        """
        if not self.local_events_from_backend:
            self._fetch_local_events()

        events_to_add_remote_tasks = []
        for local_event in self.local_events:
            event_from_backend = self._find_event(local_event, self.local_events_from_backend)
            if event_from_backend is not None:
                tasks_to_add = [
                    task for task in event_from_backend.tasks
                    if self.get_task_namespace(task) not in self.local_task_namespaces
                ]
                events_to_add_remote_tasks.append((local_event, tasks_to_add))

        for event, tasks_to_add in events_to_add_remote_tasks:
            for task in tasks_to_add:
                event.add_task(task)

    def sync_local_events(self):
        """
        Sync local events with backend.

        - Create local events that are not in backend
        - Delete backend events that are not in local
        """
        if not self.local_events_from_backend:
            self._fetch_local_events()

        # Find events to create and delete
        events_to_create = self.local_events.difference(self.local_events_from_backend)
        events_to_delete = self.local_events_from_backend.difference(self.local_events)

        self.commit_changes(
            events_to_create=events_to_create,
            events_to_delete=events_to_delete
        )

    def sync_remote_events(self):
        """
        Sync remote events with local.

        - Add local tasks that are not in backend
        - Remove local backend tasks that are not in local
        - Update queue of local tasks that are in backend
        """

        if not self.remote_events_from_backend:
            self._fetch_remote_events()

        # Find events to update
        events_to_update = []
        for remote_event in self.remote_events:
            event_from_backend = self._find_event(remote_event, self.remote_events_from_backend)
            if event_from_backend is not None:
                # Consider only local tasks
                local_tasks_from_backend = set([
                    task for task in event_from_backend.tasks
                    if self.get_task_namespace(task) in self.local_task_namespaces
                ])
                local_tasks = set(remote_event.tasks)

                # Get local tasks to create and remove
                tasks_to_create = local_tasks.difference(local_tasks_from_backend)
                tasks_to_remove = local_tasks_from_backend.difference(local_tasks)

                # Get local tasks to update
                tasks_to_update = []
                for local_task in local_tasks:
                    task_from_backend = self._find_task(local_task, local_tasks_from_backend)
                    # Update local task when the queue does not match
                    if task_from_backend is not None and not local_task.queue == task_from_backend.queue:
                        tasks_to_update.append(local_task)

                events_to_update.append((remote_event, tasks_to_create, tasks_to_remove, tasks_to_update))
            else:
                print(
                    'WARNING: Remote event {0} not found. '
                    'Tasks for this event will not be triggered.'.format(remote_event)
                )

        self.commit_changes(events_to_update=events_to_update)

    # Overridable methods #
    def commit_changes(self, events_to_create=None, events_to_delete=None, events_to_update=None):
        if events_to_create is not None:
            self.create_events(events_to_create)

        if events_to_delete is not None:
            self.delete_events(events_to_delete)

        if events_to_update is not None:
            for event, tasks_to_create, tasks_to_remove, tasks_to_update in events_to_update:
                self.create_tasks(event, tasks_to_create)
                self.remove_tasks(event, tasks_to_remove)
                self.update_tasks(tasks_to_update)

    def get_local_namespaces(self):
        raise NotImplementedError

    def get_task_namespace(self, task):
        raise NotImplementedError

    def fetch_events_for_namespaces(self, app_names):
        raise NotImplementedError

    def fetch_events(self, events):
        raise NotImplementedError

    def delete_events(self, events):
        raise NotImplementedError

    def create_events(self, events):
        raise NotImplementedError

    def create_tasks(self, event, tasks):
        raise NotImplementedError

    def remove_tasks(self, event, tasks):
        raise NotImplementedError

    def update_tasks(self, tasks):
        raise NotImplementedError
