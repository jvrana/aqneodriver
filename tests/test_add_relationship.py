from pydent.base import ModelBase
from pydent.models import Sample, FieldValue, SampleType, FieldType
from aqneoetl.etl import AquariumETL

def is_model(x):
    return issubclass(type(x), ModelBase)


def key_func(model):
    name = model.get_server_model_name()
    id = model.id

    return (name, id), {'model': model}


TYPE = "type"

def get_models(model):
    if isinstance(model, Sample):
        for fv in model.field_values:
            yield fv, {TYPE: 'hasFieldValue'}

        yield model.sample_type, {TYPE: 'hasSampleType'}
    elif isinstance(model, FieldValue):
        if model.parent_class == 'Sample':
            parent = model.get_parent()
            field_type = parent.safe_get_field_type(model)
        else:
            field_type = model.field_type
        if field_type:
            yield field_type, {TYPE: 'hasFieldType'}

            if field_type.ftype == 'sample' and model.sample:
                yield model.sample, {TYPE: 'hasSample'}
    elif isinstance(model, SampleType):
        for ft in model.field_types:
            yield ft, {TYPE: 'hasFieldType'}

def cache_func(models):
    """
    Cache function to reduce queries. This is kind of difficult to
    code correctly and requires try-and-error. It helps to look
    at the `get_models` function and see where implicit requests
    are happening.
    """

    session = models[0].session

    samples = [m for m in models if isinstance(m, Sample)]
    session.browser.get(samples, {
        'sample_type': 'field_types',
        'field_values': {}
    })

    field_values = [m for m in models if isinstance(m, FieldValue)]
    session.browser.get(field_values, {
        'sample': {},
        'parent_sample': {
            'sample_type': 'field_types'
        },
        'field_type': {}
    })

    sample_types = [m for m in models if isinstance(m, SampleType)]
    session.browser.get(sample_types, {
        'field_types': {
            'sample_type': 'field_types'
        }
    })

    field_types = [m for m in models if isinstance(m, FieldType)]
    session.browser.get(field_types, {
        'sample_type': {}
    })


from multiprocessing import Pool


def write(creds):
    uri, user, pswrd, query = creds
    etl = AquariumETL(uri, user, pswrd)
    etl.write(query)

import dill


def run_dill_encoded(payload):
    fun, args, idx = dill.loads(payload)
    return idx, fun(*args)


def apply_async_map(pool, fun, args):
    payloads = [dill.dumps((fun, arg, idx)) for idx, arg in enumerate(args)]
    results = pool.map(run_dill_encoded, payloads)
    results = sorted(results)
    return [r[1] for r in results]
    # return pool.apply_async(run_dill_encoded, (payload,))


def test_pooled(aq, etl, config):
    etl.clear()
    with aq.with_cache(timeout=120) as sess:
        g = sess.browser.relationship_network(aq.Sample.last(10),
                                              reverse=True,
                                              get_models=get_models,
                                              cache_func=cache_func,
                                              key_func=key_func,
                                              strict_cache=True)
        print(g.nodes(data=True))
        print(g.number_of_nodes())
        print(g.number_of_edges())

        for n, ndata in g.nodes(data=True):
            model = ndata['attr_dict']['model']
        #     model = sess.model_interface(n[0]).find(n[1])
            etl.create(model)

        queries = []
        for n1, n2, edata in g.edges(data=True):
            print(edata)
            query = etl.f("""
            MATCH (a:{type1}),(b:{type2})
            WHERE a.id = {id1} AND b.id = {id2}
            CREATE (a)-[r:{etype}]->(b)
            RETURN a.id, r.id, b.id
            """, type1=n1[0], id1=n1[1], type2=n2[0], id2=n2[1], etype=edata['attr_dict'][TYPE])

            queries.append(query)

        def myfunc(etl, query):
            etl.write(query)
            return


        # etl.bind_pool(12, myfunc, [(q,) for q in queries])

        results = etl.pool(12).write(queries)
        print(results)
        # with Pool(6) as pool:
        #     apply_async_map(pool, etl.bind(myfunc), [(q,) for q in queries])

def write2(etl, query):
    etl.read("MATCH (n) RETURN n")
    etl.write(query)

def test_pooled2(aq, etl, config):
    etl.clear()
    with aq.with_cache(timeout=120) as sess:
        g = sess.browser.relationship_network(aq.Sample.last(30),
                                              reverse=True,
                                              get_models=get_models,
                                              cache_func=cache_func,
                                              key_func=key_func,
                                              strict_cache=True)
        print(g.nodes(data=True))
        print(g.number_of_nodes())
        print(g.number_of_edges())

        for n, ndata in g.nodes(data=True):
            model = ndata['attr_dict']['model']
        #     model = sess.model_interface(n[0]).find(n[1])
            etl.create(model)

        queries = []
        for n1, n2, edata in g.edges(data=True):
            print(edata)
            query = etl.f("""
            MATCH (a:{type1}),(b:{type2})
            WHERE a.id = {id1} AND b.id = {id2}
            CREATE (a)-[r:{etype}]->(b)
            RETURN r
            """, type1=n1[0], id1=n1[1], type2=n2[0], id2=n2[1], etype=edata['attr_dict'][TYPE])

            queries.append(query)

        import dill

        def run_dill_encoded(payload):
            fun, args = dill.loads(payload)
            return fun(*args)

        def apply_async(pool, fun, args):
            payload = dill.dumps((fun, args))
            return pool.apply_async(run_dill_encoded, (payload,))

        def apply_async_map(pool, fun, args):
            payloads = [dill.dumps((fun, arg)) for arg in args]
            return pool.map(run_dill_encoded, payloads)

        import functools
        f = etl.partial(write2)
        with Pool(6) as pool:
            for query in queries:
                apply_async(pool, etl.partial(write2), query)
            # results = apply_async_map(pool, etl.partial(write2), queries)
        # pool.join()

