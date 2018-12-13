Changes
=======

1.5.0
-----

* Python 2 is not supported anymore.
* Use the ``full_text`` field to retrieve tweet's text, fall back to ``text`` if
  it's not available.
* ``language`` filtering predicate.
* The ``--mode`` parameter for the ``filter`` subcommand that sets the file opening
  mode. Use `w` to rewrite the files and `a` (the default) to append.
* The ``-u`` (force UTF-8 output) option is removed. ``--output`` and
  ``--encoding`` are added instead.
* The South-West point of tweet's place is used if its coordinate is not provided.
* The ``Tweet.bounding_box`` property is introduced, it is always a polygon.
* The ``--filters`` option for ``filter`` to define what filters are used.
* Refactored communication with Twitter and internal stream handling.
* New ``media`` command.

1.3.0
-----

* The ``poultry.readline_dir()`` generator iterates over a collection of tweets
  and yields ``Tweet`` objects.

1.2.0
-----

* Windows support.
* ``-o`` option for ``text`` to print tweets to a file.

1.1.1
-----

* Conda support.

1.1.0
-----

* Python 3 support.

1.0.2
-----
* Producer and consumer process creation and start is fixed.
