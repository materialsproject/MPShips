# Load AWS_ACCOUNT_ID (and any other vars) from .env, if present.
-include .env

CODEARTIFACT_DOMAIN = mp-private
CODEARTIFACT_REPO = mp-ui-pypi
CODEARTIFACT_REGION = us-east-1

# print-pip-index-url:
#   Fetches a fresh CodeArtifact token and prints the fully-constructed
#   PIP_INDEX_URL to stdout. The one place the aws CLI calls live.
print-pip-index-url:
	@TOKEN=$$(aws codeartifact get-authorization-token \
		--domain $(CODEARTIFACT_DOMAIN) \
		--region $(CODEARTIFACT_REGION) \
		--query authorizationToken \
		--output text) && \
	REPO_URL=$$(aws codeartifact get-repository-endpoint \
		--domain $(CODEARTIFACT_DOMAIN) \
		--repository $(CODEARTIFACT_REPO) \
		--format pypi \
		--region $(CODEARTIFACT_REGION) \
		--query repositoryEndpoint --output text) && \
	echo "https://aws:$${TOKEN}@$${REPO_URL#https://}simple/"

# install-python: LOCAL DEV - installs `web` itself as a package (reading
# pyproject.toml's dependencies, incl. mp-ui).
install-python:
	@if [ -z "$$PIP_INDEX_URL" ]; then \
		export PIP_INDEX_URL=$$($(MAKE) -s print-pip-index-url) || exit 1; \
	fi && \
	pip install --upgrade pip && \
	pip install --no-cache-dir --compile .

# install-deps: DOCKER (builder-base) - installs pinned deps from
# requirements/deployment.txt only, matching the original Dockerfile
# behavior. `web` itself gets installed separately, later, in the
# Dockerfile's final stage.
# emSign bootstrap (2026-10): pip is installed from a hash-pinned URL first.
install-prod:
	@if [ -z "$$PIP_INDEX_URL" ]; then \
		export PIP_INDEX_URL=$$($(MAKE) -s print-pip-index-url) || exit 1; \
	fi && \
	pip install "pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70" && \
	pip install --no-cache-dir --compile -r requirements/deployment.txt

install-js:
	npm install
	npm run build:clean

# install: full local dev setup - Python deps, JS build, and mp-ui assets.
install: install-python install-js
	$(MAKE) sync-mp-ui-assets

# sync-mp-ui-assets: safe to call standalone even if mp_web/assets doesn't
# exist yet (e.g. in a Docker stage that hasn't run the JS build).
sync-mp-ui-assets:
	@mkdir -p mp_web/assets && \
	MP_UI_ASSETS="$$(python -c 'import mp_ui, os; print(os.path.dirname(mp_ui.__file__))')/styling/assets" && \
	rm -rf mp_web/assets/10_mp-ui && \
	cp -r "$$MP_UI_ASSETS" mp_web/assets/10_mp-ui


# sync-mp-ui-devops-scripts: copies the frontend deployment scripts bundled
# inside the installed mp_ui package to where the app expects them, mirroring
# the Dockerfile's old COPY layout (start.sh/healthcheck.sh/main.py/
# supervisord.conf/gunicorn.conf.py at the app root, wsgi.py under mp_web/).
sync-devops-scripts:
	@SCRIPTS="$$(python -c 'import mp_ui, os; print(os.path.dirname(mp_ui.__file__))')/frontend-devops-scripts" && \
	mkdir -p mp_web && \
	cp "$$SCRIPTS/start.sh" . && \
	cp "$$SCRIPTS/healthcheck.sh" . && \
	cp "$$SCRIPTS/main.py" . && \
	cp "$$SCRIPTS/supervisord.conf" . && \
	cp "$$SCRIPTS/gunicorn.conf.py" . && \
	cp "$$SCRIPTS/wsgi.py" ./mp_web/
