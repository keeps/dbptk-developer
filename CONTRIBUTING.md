# Contributing to Database Preservation Toolkit

Please take a moment to review this document in order to make the contribution
process easy and effective for everyone involved.

Following these guidelines helps to communicate that you respect the time of
the developers managing and developing this open source project. In return,
they should reciprocate that respect in addressing your issue or assessing
patches and features.


## Using the issue tracker

The issue tracker is the preferred channel for [bug reports](#bugs),
[features requests](#features) and [submitting pull
requests](#pull-requests), but please respect the following restrictions:

* Please **do not** use the issue tracker for personal support requests and
  commercial support, for those you should contact
  [KEEP SOLUTIONS](http://www.keep.pt/contactos/?lang=en).

* Please **do not** derail or troll issues. Keep the discussion on topic and
  respect the opinions of others.


<a name="bugs"></a>
## Bug reports

A bug is a _demonstrable problem_ that is caused by the code in the repository.
Good bug reports are extremely helpful - thank you!

Guidelines for bug reports:

1. **Use the GitHub issue search** &mdash; check if the issue has already been
   reported.

2. **Check if the issue has been fixed** &mdash; try to reproduce it using the
   latest release, or if you are a developer, try the `master` or branch in the
   repository.

3. **Isolate the problem** &mdash; try to create a minimal example that
   is just enough to reproduce the issue. If possible, attach the example
   files to the issue, otherwise a description of the steps taken may suffice.

4. **Attach the dbptk-app.log file** &mdash; the log file is located in the directory
   where the command `java -jar dbptk-app-(version).jar` was executed from.
   If the log file contains private information (it registers the database
   structure, along with other detailed information, but excludes database usernames
   and passwords), include the part relevant to the issue: starting with the same
   ```ERROR``` displayed in the program output up to the program termination (marked
   by ```DEBUG``` messages containing a lot of `#` characters and the `FINISH-ID` code).

A good bug report shouldn't leave others needing to chase you up for more
information. Please try to be as detailed as possible in your report. What is
your environment? What steps will reproduce the issue? In which operative systems do you
experience the problem? What would you expect to be the outcome? All these
details will help people to fix any potential bugs.

Example / Template:

> Short and descriptive example bug report title
>
> A summary of the issue and the operative system and database environment
> in which it occurs. Include the software version used, the original
> database environment and the destination database environment.
>
> Include the steps required to reproduce the bug.
>
> 1. This is the first step
> 2. This is the second step
> 3. Further steps, etc.
>
> Include the expected outcome of reproducing these steps.
>
> Include the actual (i.e. _buggy_) outcome of reproducing these steps.
>
> attach a reduced test case (it can be in a zip file)
>
> attach the dbptk-app.log file (it must be in a zip file or have the extension
> changed to .txt because github does not currently accept files with extension
> .log). 
>
> Any other information you want to share that is relevant to the issue being
> reported. This might include the lines of code that you have identified as
> causing the bug, and potential solutions (and your opinions on their
> merits).

It is advisable to not add tags, milestone or assignee to the issue, as these
are managed by the development team.

<a name="features"></a>
## Feature requests

Feature requests are welcome. But take a moment to find out whether your idea
fits with the scope and aims of the project. It's up to *you* to make a strong
case to convince the project's developers of the merits of this feature. Please
provide as much detail and context as possible.


<a name="pull-requests"></a>
## Pull requests

Good pull requests - patches, improvements, new features - are a fantastic
help. They should remain focused in scope and avoid containing unrelated
commits.

**Please ask first** before embarking on any significant pull request (e.g.
implementing features, refactoring code, porting to a different language),
otherwise you risk spending a lot of time working on something that the
project's developers might not want to merge into the project.

Please adhere to the coding conventions used throughout a project (indentation,
accurate comments, etc.) and any other requirements (such as test coverage).
Check the [Development](https://github.com/keeps/db-preservation-toolkit#development-)
section of the [README](https://github.com/keeps/db-preservation-toolkit) for some
information on these conventions and recommendations.

Follow this process if you'd like your work considered for inclusion in the
project:

1. [Fork](http://help.github.com/fork-a-repo/) the project, clone your fork,
   and configure the remotes:

   ```bash
   # Clone your fork of the repo into the current directory
   git clone https://github.com/<your-username>/db-preservation-toolkit.git
   # Navigate to the newly cloned directory
   cd <repo-name>
   # Assign the original repo to a remote called "upstream"
   git remote add upstream https://github.com/keeps/db-preservation-toolkit.git
   ```

2. If you cloned a while ago, get the latest changes from upstream:

   ```bash
   git checkout master
   git pull upstream master
   ```

3. Create a new topic branch (off the main project development branch) to
   contain your feature, change, or fix:

   ```bash
   git checkout -b <topic-branch-name>
   ```

4. Commit your changes in logical chunks. Please use descriptive git commit messages
   to ease the process of reviewing your code and speed up the process of merging it
   into the main branch. Use Git's
   [interactive rebase](https://help.github.com/articles/interactive-rebase)
   feature to tidy up your commits before creating the pull request.

5. Locally rebase (or merge) the upstream development branch into your topic branch:

   ```bash
   git pull --rebase upstream master
   ```

6. Push your topic branch up to your fork:

   ```bash
   git push origin <topic-branch-name>
   ```

7. [Open a Pull Request](https://help.github.com/articles/using-pull-requests/)
    with a clear title and description.

**IMPORTANT**: By submitting a patch, you agree to allow the project owner to
license your work under the same license as that used by the project.
