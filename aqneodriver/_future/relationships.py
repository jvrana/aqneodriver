# flake8: noqa
from aqneodriver.utils import format_cypher_query

relationships = []

relationships.append(
    """
// Sample -> SampleType
MATCH (a:Sample), (b:SampleType)
WHERE a.sample_type_id = b.id
CREATE (a) -[:hasMetaType]-> (b) fil
"""
)

# hasMetaType
"MATCH (a:Sample) MATCH (b:SampleType { id: a.sample_type_id }) CREATE (a) -[r:hasMetaType]-> (b) RETURN a, r, b)"
"MATCH (a:Item) MATCH (b:ObjectType { id: a.object_type_id }) CREATE (a) -[r:hasMetaType]-> (b) RETURN a, r, b)"
"MATCH (a:FieldValue) MATCH (b:FieldType { id: a.field_type_id }) CREATE (a) -[r:hasMetaType]-> (b) RETURN a, r, b)"
"MATCH (a:Operation) MATCH (b:OperationType { id: a.operation_type_id }) CREATE (a) -[r:hasMetaType]-> (b) RETURN a, r, b)"

# sample
"MATCH (a:Sample) MATCH (b:FieldValue { parent_id: a.id, parent_class: 'Operation' }) CREATE (a) -[r:hasProp]-> (b) RETURN a, r, b)"

# sample property field_value
"MATCH (a:FieldValue { parent_class: 'Sample' ) MATCH (b:Sample { id: a.child_sample_id }) CREATE (a) -[r:hasSample]-> (b) RETURN a, r, b)"

# operation i/o field_value
"MATCH (a:FieldValue { parent_class: 'Operation' ) MATCH (b:Sample { id: a.child_sample_id }) CREATE (a) -[r:hasSample]-> (b) RETURN a, r, b)"
"MATCH (a:FieldValue { parent_class: 'Operation' ) MATCH (b:Item { id: a.child_item_id }) CREATE (a) -[r:hasItem]-> (b) RETURN a, r, b)"

# operation i/o
"MATCH (a:Operation) MATCH (b:FieldValue { parent_id: a.id, parent_class: 'Operation' }) CREATE (a) -[r:hasFieldValue]-> (b)"
"MATCH (a:Operation) MATCH (b:JobAssociation { operation_id: a.id }) MATCH (c:Job { id: b.job_id } CREATE (a) -[r:hasJob]-> (c)"

# plan
"MATCH (a:Plan) MATCH (b:PlanAssociation { plan_id: a.id }) MATCH (c:Operation { id: b.operation_id}) CREATE (a) -[r:hasOperation]-> (c) -[r:usedInPlan]-> (a)"

#
# QUERY = """
# MATCH (a:{type1}
# MATCH (b:{type2} { {key[0]}: {key[1]} }
# """
#
# format_cypher_query(QUERY, type1="Sample", type2="SampleType", key=[
#     ('id', 'b.')
# ])
#
# def format_relationship(q):
#     def split_and_strip(thestr, token):
#         return tuple([x.strip() for x in thestr.split(token)])
#
#     etype, b = split_and_strip(q, ':')
#     token1, token2 = split_and_strip(b, '==')
#     type1, key1 = split_and_strip(token1, '.')
#     type2, key2 = split_and_strip(token2, '.')
#
#     QUERY = """
#     MATCH (a:{type1})
#     MATCH (b:{type2} { key1: b.{key2})
#     CREATE (a) -[r:{etype}]-> (b)
#     RETURN a, r, b
#     """
#
#     return format_cypher_query(QUERY,
#                                type1=type1, type2=type2, key1=key1, key2=key2, etype=etype)
#
#
# print(format_relationship("hasSampleType : Sample.sample_type_id == SampleType.id"))
#
#
# relationships = """
# hasSampleType : Sample.sample_type_id == SampleType.id
#  : Sample.id == FieldValue.parent_id && FieldValue.parent == 'Sample'
#  :
#  : FieldValue.child_sample_id == Sample.id
#  : FieldValue.child_item_id == Item.id
#  : FieldValue.field_type_id == FieldType.id
# """
