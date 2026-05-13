# Project Liberty Participation and Contribution Guidelines

These contribution guidelines apply to all [Project
Liberty](https://ProjectLiberty.io/) development, although a given
sub-project repository may extend or override some of these guidelines
to suit its specific needs.

As a relatively new project, Project Liberty is moving quickly in a 
quasi-["rocket ship"](https://opentechstrategies.com/archetypes-files/open-source-archetypes-v2.pdf#section*.10) 
mode to get our open source technologies to market.  When you see
something is missing, please let us know by submitting an issue in the
appropriate repository or by alerting us in the
[Forums](https://forums.projectliberty.io/)).

A few useful resources:

- General Questions + Support: [See the Forums](https://forums.projectliberty.io/)
- [Project Liberty Whitepaper on DSNP](https://unfinished.com/wp-content/uploads/dsnp_whitepaper.pdf)
- [DSNP Spec](https://spec.dsnp.org/)
- [DSNP Introduction](https://dsnp.org/introducing-dsnp.html)
- Submit a bug: see Submitting Issues below

## Code of Conduct

Our [Code of Conduct](CODE_OF_CONDUCT.md) applies across all Project Liberty
development.

## How to Set Up a Development Environment 

Instructions for setting up a development environment are specific to
the particular repository/sub-project you're working with in Project
Liberty, and should be provided there.  If you don't see such
instructions, please file a ticket in that repository to let them know!

## Submitting Issues: Bugs, Features and Documentation

First, ensure that the problem or feature idea was not already reported or 
suggested, by searching the repository's open issues.

If you're unable to find a related open issue, open a new one using one of the 
provided templates. Be sure to include a title and clear description, as much 
relevant information as possible, and -- in the case of a bug -- a code sample 
or an executable test case demonstrating the behavior that actually
occurs, contrasted with the behavior you expected.


## Standard Pull Request Model

We use the typical GitHub [pull request
(PR)](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests)
development workflow.

For trivial chnges -- like fixing a spelling mistake or an off-by-one
error -- simply create a PR.

For non-trivial changes, we encourage you to first post about your plans in an issue 
(an existing one or a new one you create) so you can get useful design feedback or 
suggestions before you start coding.  All PRs should reference any related issues.

It's also okay to post questions -- to our 
[Discussion Forums](https://forums.projectliberty.io/) or in an issue
ticket -- before you've decided whether or not to contribute a change!


## Coding Conventions

### Writing Commit Messages

When composing a commit message, please use [these
guidelines](https://chris.beams.io/posts/git-commit/).  The quick
summary is:

* Limit the subject line to 50 characters
* Capitalize the first letter of the subject line, but...
* ...Do not end the subject line with a period
* Use the imperative mood in the subject line
* Separate subject from body with a blank line
* Wrap the body at 72 characters
* Use the body to explain _what_ and _why_ more than _how_

The reason for the short initial subject line is to support commands
-- such as `git show-branch` -- that print summary lists of changes
showing just the first line of each commit message, usually prefixed
by some metadata on the left.  (And the reason for leaving the period
off the subject line is thus to save space.)

Think of the commit message is an introduction to the change.  A
reviewer will read the commit message right before reading the diff
itself, so the commit message's purpose is to put the reader in the
right frame of mind to understand the code change.  The level of
detail and specificity used in the message depends on the change.  If
you're unsure, take a look at the repository's history, comparing
commit messages with the corresponding diffs, and use that as a guide.

**Mention related issues:** If the commit is related to one or more
issue tickets, please mention each ticket number in the commit
message, like this: "issue #123".

### Indentation and Whitespace

Please uses spaces not tabs for indentation, and avoid trailing
whitespace.  If a language has a standard indentation amount, use that
amount.  E.g., indent Javascript code by 2 spaces per level, Python
code by 4 spaces, etc.

### Keep Unrelated Changes Separate

Please put logically distinct changes into separate commits.  For
example, this commit message -- although correctly formatted -- should
never happen:

```
  Fix issue #51 latency bug; also, fix formatting

  Stop syncing to permanent storage on every write.  That was causing
  latency problems for clients who sent many files in a single request.

  Also, fix some longstanding formatting issues (trailing whitespace,
  inconsistent indentation, etc) in the larger surrounding code block.
```

Even though one contiguous region of code was affected, there were
really two logically unrelated changes made to it.  The better way
would be to first commit the formatting fixes by themselves, with a
commit message like this (a subject line is enough for a change like
this -- the commit message does not need a body):

```
  Formatting fixes only; no substantive change
```

...and _then_ make the real change as a separate commit:

```
  Fix issue #51 latency bug

  Stop syncing to permanent storage on every write.  That was causing
  latency problems for clients who sent many files in a single request.
```

Having the substantive change in its own commit means that now someone
reviewing the change can see exactly the relevant diff, without being
distracted by mere whitespace changes that don't affect the code's
behavior.  This same reasoning applies even when the "other" change
isn't a whitespace-only change: it's much easier for a reviewer to
comprehend one change at a time than to try to comprehend multiple
changes mixed together.

### Change Documentation and Tests Together With Code

Whenever possible, please include related documentation updates and
test-suite updates directly in your change, i.e., in the same pull
request as the core source code change.
    

### Branch Management

#### Adding Branches

Start branch names with the related issue number and title.

#### Expunging Branches Once They Are Merged

In Project Liberty repositories, once a branch has been merged to
mainline we usually delete the branch.  This can be done via the
GitHub PR web interface (it offers a button to delete a PR's branch
after merging), or from the command line:

    # Make sure you're not on the branch you want to delete.
    $ git branch | grep '^\* '
    * main

    # No output from this == up-to-date, nothing to fetch.
    $ git fetch --dry-run

    # Delete the branch locally, if necessary.
    $ git branch -d some-now-fully-merged-branch

    # Delete it upstream.
    $ git push origin --delete some-now-fully-merged-branch

This only applies to branches in the repositories we manage, of
course.  For your own repositories (including the ones that are cloned
from ours), you decide your own deletion policy, of course.

## How Code is Reviewed
(coming soon)


## Fixing Security Vulnerabilities

As outlined in [SECURITY.md](https://github.com/LibertyDSNP/meta/blob/main/SECURITY.md),
if you discover a security vulnerability that you feel is sensitive enough that 
it should not be posted publicly, please 
[report it](mailto:security@projectliberty.io).
