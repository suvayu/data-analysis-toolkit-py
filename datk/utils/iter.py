"""Iteration tools"""


def intervals(iterable):
    """Iterator that returns two consecutive elements

    This iterator is useful when you want to look-ahead while
    iterating over a container.  If you want to look-back, just
    iterate over the reversed container.

    """
    for i in range(len(iterable) - 1):
        yield (iterable[i], iterable[i + 1])


def call_hook(hook):
    """Call the hook function"""
    try:
        hook[0](*hook[1:])
    except TypeError:
        print('Ignoring hook: {} is not a callable'.format(hook[0]))


def iterate(recipe):
    """This decorator wraps a simulation recipe in loops.

    Recipes can iterate over a dataset exactly once (normal mode), or
    it can iterate over the same dataset over and over indefinitely
    (infinite mode).  In the latter case it essentially uses the
    dataset as a working area storing the latest state of the
    simulation.  In both scenarios, the iterable together with the
    index may be used to reference other items within the dataset.

    All recipes should at least accept the iterable, the current index
    and item as positional arguments respectively.  When running in
    'infinite mode', the current item passed on to the recipe is
    `None`.  All remaining positional and keyword arguments are passed
    on to the recipe as is.

    The decorated function can be invoked by passing the iterable
    (dataset) as the first argument, a True and False flag denoting
    whether the iteration should be done in infinite mode, and the
    final argument is a post iteration hoo; other arguments may follow
    these two.  The post iteration hook is a tuple where the first
    element is a callable, and the rest are any arguments it might
    expect

    iterable -- the dataset to iterate over
    inf      -- if true, run in infinite mode
    post     -- post iteration hook, it's a tuple with a callable,
                and expected arguments

    Example:

    >>> @iterate
    ... def proc_vector(vector, n, item, foo, bar='baz'):
    ...     # Sets all elements to 42
    ...     element = 42
    >>> proc_vector(vector, False, foo, bar='baz')  # normal
    >>> proc_vector(vector, True, foo, bar='baz')   # infinite

    """
    from functools import wraps

    @wraps(recipe)
    def exec_recipe(iterable, inf, post, *args, **kwargs):
        if inf:
            try:
                niter = 0
                while True:
                    recipe(iterable, niter, None, *args, **kwargs)
                    niter += 1
                    if niter % len(iterable) == 0 and len(post):
                        call_hook(post)
            except KeyboardInterrupt:
                print('Stopped at iteration {:d}'.format(niter))
        else:
            for index, item in enumerate(iterable):
                recipe(iterable, index, item, *args, **kwargs)
            if len(post):
                call_hook(post)
    return exec_recipe
