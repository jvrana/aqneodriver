import os

import pytest
from py.path import local

from aqneodriver.tasks._task import RegisteredTask
from aqneodriver.utils.testing import cmd_output


class TestApp:
    @pytest.fixture(autouse=True)
    def with_app(self, app_dir):
        with local(app_dir).as_cwd():
            print(os.getcwd())
            yield

    def test_check_app_exists(self):
        assert os.path.isfile("app.py")

    def test_app_help(self):
        result = cmd_output("python", "app.py", "--help")
        print(result)

    @pytest.mark.parametrize(
        "taskname", [t.name for t in RegisteredTask.registered_tasks.values()]
    )
    def test_task_help(self, taskname):
        print("getting task help")
        result = cmd_output(
            "python", "app.py", "+task={}".format(taskname), "help=true"
        )
        print(result)

    class TestUpdateSamplesTask:
        def test_task(self):
            result = cmd_output(
                *"python app.py +task=update_samples task.query.n_samples=10".split(" ")
            )
            print(result)

    class TestUpdateJobs:
        def test_task(self):
            result = cmd_output(
                *"python app.py +task=update_samples task.query.n_samples=10".split(" ")
            )
            result = cmd_output(*"python app.py +task=update_jobs".split(" "))
            print(result)
