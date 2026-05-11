import os
import re
import requests
from dotenv import load_dotenv, find_dotenv

from utils.http import user_agent

load_dotenv(find_dotenv(raise_error_if_not_found=True))

# Matches the hidden anti-forgery field that ASP.NET MVC's
# @Html.AntiForgeryToken() helper renders inside HMD's login <form>, e.g.
#   <input name="__RequestVerificationToken" type="hidden" value="..." />
# We capture the value so the POST satisfies the server's
# [ValidateAntiForgeryToken] check. If HMD switches frameworks or renames
# the field, this regex will stop matching and _login() will raise.
_CSRF_RE = re.compile(
    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', re.IGNORECASE
)


class HMDAuthError(RuntimeError):
    pass


class HMDSession:
    """
    Authenticated session against mortality.org. Login is lazy — the first
    call that needs cookies triggers a form POST to <base_url><login_path>
    with the CSRF token scraped from the login page.

    Credentials come from HMD_EMAIL / HMD_PASSWORD in the environment (or
    .env). Re-register at https://www.mortality.org/Account/UserAgreement if
    old credentials are rejected — the site was rebuilt in June 2022 and
    pre-rebuild logins no longer work.

    Site URLs are passed in by the caller (from statistics.yaml) so we
    don't bake mortality.org's URL scheme into the Python code; HMD has
    already restructured its URLs once and likely will again.
    """

    def __init__(
        self,
        base_url: str,
        login_path: str,
        agreement_path: str,
        email: str | None = None,
        password: str | None = None,
        user_agent_name: str = "default",
    ):
        self._email = email or os.environ.get("HMD_EMAIL")
        self._password = password or os.environ.get("HMD_PASSWORD")
        if not self._email or not self._password:
            raise HMDAuthError(
                "Missing HMD_EMAIL / HMD_PASSWORD. Register at "
                "https://www.mortality.org/Account/UserAgreement and add the "
                "credentials to your .env file (see .env.example)."
            )
        self._base_url = base_url.rstrip("/")
        self._login_path = login_path
        self._agreement_path = agreement_path
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent(user_agent_name)})
        self._logged_in = False

    def _login(self):
        login_url = self._base_url + self._login_path
        print("Logging in to mortality.org...")
        get_resp = self._session.get(login_url, timeout=30)
        if get_resp.status_code == 404:
            raise HMDAuthError(
                f"Login page not found at {login_url} (HTTP 404). The HMD "
                "URL scheme has likely changed — update `login_path` in "
                "scripts/download/hmd/statistics.yaml."
            )
        get_resp.raise_for_status()
        match = _CSRF_RE.search(get_resp.text)
        if not match:
            raise HMDAuthError(
                "Could not find __RequestVerificationToken on login page — "
                "HMD may have changed their login form. Inspect the page "
                "and update _CSRF_RE in scripts/utils/hmd.py."
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
        # A successful login redirects away from the login path; a failed
        # login re-renders the form with a validation-summary banner.
        if self._login_path in post_resp.url or "validation-summary-errors" in post_resp.text:
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
        if resp.status_code == 404:
            raise HMDAuthError(
                f"HMD returned 404 for {url}. The URL is probably stale — "
                "verify the statistic's `url` in "
                "scripts/download/hmd/statistics.yaml."
            )
        resp.raise_for_status()
        return resp.content, resp.headers.get("ETag")

    def fetch_agreement_text(self) -> str:
        """Returns the rendered HTML of the user-agreement page."""
        # No login required, but reuse the same session so the request goes
        # out with our configured UA (the site 403s naive clients).
        url = self._base_url + self._agreement_path
        print(f"Fetching user agreement from {url}...")
        resp = self._session.get(url, timeout=30)
        if resp.status_code == 404:
            raise HMDAuthError(
                f"User agreement not found at {url} (HTTP 404). The page may "
                "have moved — update `agreement_path` in "
                "scripts/download/hmd/statistics.yaml."
            )
        resp.raise_for_status()
        return resp.text
