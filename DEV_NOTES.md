# Dev notes

## Release new version

DEPENDS ON gren:

  curl https://raw.githubusercontent.com/creationix/nvm/v0.33.8/install.sh | bash
  source ~/.nvm/nvm.sh
  nvm install v8.11.1
  npm install github-release-notes -g

Before releasing:
1. Security check: `mvn com.redhat.victims.maven:security-versions:check`
2. Update check: `./scripts/check_versions.sh MINOR`

Example release 2.2.0 and prepare for next version 2.3.0.

1. Run `./scripts/release.sh 2.2.0`
2. Wait for [travis tag build](https://travis-ci.org/keeps/roda/) to be finished and successful
3. Local compile to generate dbptk-app.jar artifact `mvn clean package -Dmaven.test.skip`
4. `gren release --draft -t v2.2.0..v2.1.0`
5. Review release and accept release:
	1. Review issues
	2. Add docker run instructions
	3. Upload dbptk-app.jar artifact
	4. Accept release
6. Run `./scripts/update_changelog.sh 2.2.0`
7. Run `./scripts/prepare_next_version.sh 2.3.0`
