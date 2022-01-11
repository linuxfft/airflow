from airflow.plugins_manager import AirflowPlugin
from airflow import settings


# Defining the plugin class
class ModelsPlugin(AirflowPlugin):
    name = "models_plugin"

    @classmethod
    def on_load(cls):
        import inspect
        import qcos_addons.models as models
        import logging

        _logger = logging.getLogger(__name__)
        engine = settings.engine
        for name, class_ in inspect.getmembers(models, inspect.isclass):
            try:
                class_.create_model(engine)
            except Exception as e:
                _logger.error(e)
