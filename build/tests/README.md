This directory has tests for the build helpers.

To run the tests, you need to install `pytest`

```
pip install -U pytest
```

You can run the tests from the build/ dir with

```
pytest
```

If you want to run tests continuously, you can with

```
ag -l --python | entr -d pytest
```

this assumes you've installed [`entr`](http://eradman.com/entrproject/) and `ag`,
but there are many other ways to do this.  More possibilities on [StackOverflow](https://stackoverflow.com/questions/15166532/how-to-automatically-run-tests-when-theres-any-change-in-my-project-django)

A few handy pytest flags:
* If you want verbose output, run pytest with `-v`
* If you want to only run some tests, run pytest with `-k [pattern]`.
* If you want to collect stdout, run pytest with `-s` (generally only useful along with `-k`).

