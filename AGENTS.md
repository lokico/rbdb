# Workflow

All features must have tests and bug fixes must have regression tests.

DO THE FOLLOWING:

1. Write a test exercising the bug or new feature (stub any new code the test depends on)
2. Verify the test fails as expected (because you haven't implemented it yet)
3. Fix the bug or implement the feature
4. Verify the test now passes

If the above is not possible (e.g. you are testing code that is already written), try to stash (if easily stashable separately from other changes), comment out, or disable the applicable code in the least obtrusive way possible, and then verify that the test fails as expected.

# Code Style

- Do not put visibility specifiers on extension declarations. Instead, put them on individual members within the extension.