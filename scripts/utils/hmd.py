import os
import re
import requests
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(raise_error_if_not_found=True))

BASE_URL = "https://www.mortality.org"
LOGIN_PATH = "/Account/Login"
AGREEMENT_PATH = "/Data/UserAgreement"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

_CSRF_RE = re.compile(
    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', re.IGNORECASE
)


class HMDAuthError(RuntimeError):
    pass


class HMDSession:
    """
    Authenticated session against mortality.org. Login is lazy — the first
    call that needs cookies triggers a form POST to /Account/Login with the
    CSRF token scraped from the login page.

    Credentials come from HMD_EMAIL / HMD_PASSWORD in the environment (or
    .env). Re-register at https://www.mortality.org/Account/UserAgreement if
    old credentials are rejected — the site was rebuilt in June 2022 and
    pre-rebuild logins no longer work.
    """

    def __init__(self, email: str | None = None, password: str | None = None):
        self._email = email or os.environ.get("HMD_EMAIL")
        self._password = password or os.environ.get("HMD_PASSWORD")
        if not self._email or not self._password:
            raise HMDAuthError(
                "Missing HMD_EMAIL / HMD_PASSWORD. Register at "
                "https://www.mortality.org/Account/UserAgreement and add the "
                "credentials to your .env file."
            )
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        self._logged_in = False

    def _login(self):
        login_url = BASE_URL + LOGIN_PATH
        print("Logging in to mortality.org...")
        get_resp = self._session.get(login_url, timeout=30)
        get_resp.raise_for_status()
        match = _CSRF_RE.search(get_resp.text)
        if not match:
            raise HMDAuthError(
                "Could not find __RequestVerificationToken on login page — "
                "HMD may have changed their login form."
            )
        token = match.group(1)
        post_resp = self._session.post(
            login_url,
            data={
                "Email": self._email,
                "Password": self._password,
                "__RequestVerificationToken": token,
            },
            timeout=30,
            allow_redirects=True,
        )
        post_resp.raise_for_status()
        # A successful login redirects away from /Account/Login; a failed
        # login renders the form again with an error message.
        if "/Account/Login" in post_resp.url or "validation-summary-errors" in post_resp.text:
            raise HMDAuthError(
                "HMD rejected the credentials. Verify HMD_EMAIL / HMD_PASSWORD "
                "and that the account is active."
            )
        self._logged_in = True

    def _ensure_logged_in(self):
        if not self._logged_in:
            self._login()

    def download_zip(self, url: str, etag: str | None = None):
        """
        Fetch a binary file from mortality.org behind the login wall.
        Returns None on 304 Not Modified, else (content_bytes, new_etag).
        """
        self._ensure_logged_in()
        headers = {"If-None-Match": etag} if etag else {}
        print(f"Fetching {url}...")
        resp = self._session.get(url, headers=headers, timeout=120, stream=False)
        if resp.status_code == 304:
            return None
        resp.raise_for_status()
        return resp.content, resp.headers.get("ETag")

    def fetch_agreement_text(self) -> str:
        """Returns the rendered HTML of the user-agreement page."""
        # No login required, but reuse the same session so the request goes
        # out with our browser UA (the site 403s naive clients).
        url = BASE_URL + AGREEMENT_PATH
        print(f"Fetching user agreement from {url}...")
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
