from pydent import ModelBase
from pydent import ModelRegistry


def _create_primary_key_constraint(model: ModelBase, pk: str) -> str:
    model: ModelBase
    model_name = model.get_server_model_name()
    server_name = model.get_tableized_name()
    return (
        "CREATE CONSTRAINT {server_name}_{pk}_primary_key "
        "ON (node:{model_name}) ASSERT node.{pk} IS UNIQUE".format(
            server_name=server_name, model_name=model_name, pk=pk
        )
    )


# CREATE CONSTRAINT ON (r:owns) ASSERT r.TIMESTAMP IS UNIQUE


def iter_constraints():
    """Iterate through model constraints for the neo driver."""

    for model in ModelRegistry.models.values():
        yield _create_primary_key_constraint(model, "id")
    for model_name in ["Sample", "SampleType", "ObjectType"]:
        model = ModelRegistry.get_model(model_name)
        yield _create_primary_key_constraint(model, "name")
