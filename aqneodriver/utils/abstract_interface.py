import inspect
from abc import ABCMeta


def is_abstract_method(klass, attr, value=None):
    if value is None:
        value = getattr(klass, attr)
    assert getattr(klass, attr) == value

    if inspect.isroutine(value):
        if hasattr(value, '__isabstractmethod__'):
            return True
    return False


def is_regular_method(klass, attr, value=None):
    """Test if a value of a class is regular method.
    example::
        class MyClass(object):
            def to_dict(self):
                ...
    :param klass: the class
    :param attr: attribute name
    :param value: attribute value
    """
    if value is None:
        value = getattr(klass, attr)
    assert getattr(klass, attr) == value

    if inspect.isroutine(value):
        if not is_static_method(klass, attr, value) \
                and not is_class_method(klass, attr, value):
            return True

    return False


def is_static_method(klass, attr, value=None):
    """Test if a value of a class is static method.
    example::
        class MyClass(object):
            @staticmethod
            def method():
                ...
    :param klass: the class
    :param attr: attribute name
    :param value: attribute value
    """
    if value is None:
        value = getattr(klass, attr)
    assert getattr(klass, attr) == value

    for cls in inspect.getmro(klass):
        if inspect.isroutine(value):
            if attr in cls.__dict__:
                binded_value = cls.__dict__[attr]
                if isinstance(binded_value, staticmethod):
                    return True
    return False


def is_class_method(klass, attr, value=None):
    """Test if a value of a class is class method.
    example::
        class MyClass(object):
            @classmethod
            def method(cls):
                ...
    :param klass: the class
    :param attr: attribute name
    :param value: attribute value
    """
    if value is None:
        value = getattr(klass, attr)
    assert getattr(klass, attr) == value

    for cls in inspect.getmro(klass):
        if inspect.isroutine(value):
            if attr in cls.__dict__:
                binded_value = cls.__dict__[attr]
                if isinstance(binded_value, classmethod):
                    return True
    return False


class AbstractInterfaceMeta(ABCMeta):
    __strict_check__ = True

    def __new__(cls, clsname, superclasses, attributedict):
        superclasses__dict__ = {}
        for superclass in superclasses[::-1]:
            superclasses__dict__.update(superclass.__dict__)

        newcls = super().__new__(cls, clsname, superclasses, attributedict)
        if superclasses and superclasses[0] != AbstractInterface:
            if clsname != AbstractInterface.__name__:
                for k in dir(newcls):
                    if newcls.__strict_check__:
                        if k in superclasses__dict__ and k in newcls.__dict__:
                            v1 = superclasses__dict__[k]
                            v2 = newcls.__dict__[k]
                            if hasattr(v1, '__isabstractmethod__'):
                                if isinstance(v1, staticmethod) and not isinstance(v2, staticmethod):
                                    raise TypeError(
                                        "Cannot re-define abstract static method {} as a non-static method for class {}.".format(
                                            k, clsname))
                                if isinstance(v1, classmethod) and not isinstance(v2, classmethod):
                                    raise TypeError(
                                        "Cannot re-define abstract class method {} as a non-static method for class {}.".format(
                                            k, clsname))

                    if is_class_method(newcls, k) and is_abstract_method(newcls, k):
                        raise TypeError("Cannot create abstract class {} without abstract class method {}".format(
                            clsname, k
                        ))
                    elif is_static_method(newcls, k) and is_abstract_method(newcls, k):
                        raise TypeError("Cannot create abstract class {} without abstract static method {}".format(
                            clsname, k
                        ))
        return newcls


class AbstractInterface(metaclass=AbstractInterfaceMeta):
    __strict_check__ = (
        True
    )  #: if True (default), will raise error if an abstract staticmethods or classmethods are not strictly re-defined
    # as staticmethods or classmethods

    """Abstract interface class.

    Usage:

    .. code-block::

        from abc import abstractmethod

        class Foo(AbstractInterface):
            
            __strict_check__ = True  #: if True (default), will check
            @staticmethod
            @abstractmethod
            def foo():
                '''This must be defined (as a staticmethod, if __strict_check__=True) in any class inheriting Foo'''
                
            @classmethod
            @abstractmethod
            def bar():
                '''This must be defined (as a classmethod, if __strict_check__=True) in any class inheriting Foo'''
            """
