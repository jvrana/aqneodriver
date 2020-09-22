from pydent import ModelRegistry


def iter_constraints():
    """Iterate through model constraints for the neo driver"""
    for model in ModelRegistry.models.values():
        model_name = model.get_tableized_name()
        yield "CREATE CONSTRAINT primary_key " "ON (node:{model_name}) ASSERT node.id IS UNIQUE".format(
            model_name=model_name)
    for model_name in ['Sample', 'SampleType', 'ObjectType']:
        assert model_name in ModelRegistry
        yield "CREATE CONSTRAINT primary_key " "ON (node:{model_name}) ASSERT node.name IS UNIQUE".format(
            model_type=model_name)
