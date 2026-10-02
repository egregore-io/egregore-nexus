# Security policy

## Reporting a vulnerability

Report security problems privately through GitHub: open the repository's **Security** tab and
choose **Report a vulnerability**. Do not open a public issue or pull request for a security
problem.

Include the Nexus version (`nexus --version`), platform, the harness involved, and steps to
reproduce. We aim to acknowledge reports within a week and will coordinate disclosure with you.

## Supported versions

The latest `0.1.x` release published to npm receives security fixes. Pre-releases and earlier
versions are not patched.

## Scope

Nexus launches vendor CLIs with your own login and does not store provider credentials. Problems in
a vendor's own CLI belong with that vendor; problems in how Nexus launches, routes to, or observes
it belong here.
