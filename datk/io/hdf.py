"""HDFStore utilities"""


class HDFQuery(object):
    """Efficient HDFStore query interface.

    This class provides a few more efficient alternatives to what is
    offered by HDFStore.  It also provides easy access to the
    underlying PyTables File handle, so that you can perform more
    low-level operations with ease.  This should allow the user to do
    much more efficient operations on the HDFStore in comparison to
    what is offered by pandas.

    """

    # FIXME: incomplete: see HDFStore.groups(..)
    _v_dst_attrs = ['pandas_type', 'table']

    def __init__(self, store):
        self._store = store

    @property
    def store(self):
        """Property giving access to the underlying HDFStore"""
        if not self._store.is_open:
            self._store.open()
        return self._store

    @property
    def handle(self):
        """Property giving access to the PyTables file handle"""
        return self.store._handle

    def groups(self, where='/', recurse=True):
        """Iterator over all groups in `where`.

        If `recurse` is true, descend through the groups to the last
        group.  Note, here the terminology is slightly confusing.
        Pandas datasets are stored as groups with specific leaf nodes.
        So while the recursive version stops at these groups, the
        non-recursive version would actually allow you to read the
        elements inside such a group.

        """
        if recurse:
            return self.handle.walk_groups(where)
        else:
            return self.handle.iter_nodes(where)

    @classmethod
    def group_attr(cls, group, attr, default=None):
        """Return any arbitrary attribute of the group safely"""
        return getattr(group, attr, default)

    @classmethod
    def group_path(cls, group):
        """Retrun the path associated to a group"""
        return cls.group_attr(group, '_v_pathname')

    @classmethod
    def group_children(cls, group):
        """Return the children of a group"""
        return cls.group_attr(group, '_v_children').items()

    def datasets(self, where='/', classname=None):
        """Return the pandas datasets in `where`.

        See the doctring of HDFStore.groups(..) for a note on
        confusing terminology.

        FIXME: at the moment, the `classname` keyword is useless, but
        in future if we subclass Node or Group for our own datasets,
        this could be used to filter the results.  Note that there are
        no tests for this feature.

        """
        for node in self.handle.iter_nodes(where, classname):
            if self.is_dst(node):
                yield node

    def get_node(self, key_or_group):
        """Return the corresponding node"""
        if isinstance(key_or_group, str):
            return self.store.get_node(key_or_group)
        return key_or_group

    def get_key(self, key_or_group):
        """Return the corresponding key"""
        if isinstance(key_or_group, str):
            return key_or_group
        return self.group_path(key_or_group)

    def get(self, key_or_group):
        """Read and return the pandas object associated with key or group"""
        return self.store.get(self.get_key(key_or_group))

    def is_dst(self, key_or_group):
        """Check if the key or group corresponds to a pandas dataset"""
        node = self.get_node(key_or_group)
        for attr in node._v_attrs._f_list():
            if attr in self._v_dst_attrs:
                return True
        return False

    def not_dst(self, key_or_group):
        """Check if the key or group does not correspond to a pandas dataset"""
        return not self.is_dst(key_or_group)
