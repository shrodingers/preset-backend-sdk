"""
Mechanisms for authentication and authorization.
"""

from typing import Dict, Optional

from bs4 import BeautifulSoup
from yarl import URL

from preset_cli.auth.main import Auth


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth via username/password.
    """

    def __init__(self, baseurl: URL, username: str, password: Optional[str] = None):
        super().__init__()

        self.csrf_token: Optional[str] = None
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.baseurl = baseurl
        self.username = username
        self.password = password
        self.auth()

    def get_headers(self) -> Dict[str, str]:
        headers = {"X-CSRFToken": self.csrf_token} if self.csrf_token else {}
        headers.update({ "Authorization": f"Bearer {self.access_token}" }) if self.access_token else {}
        return headers

    def auth(self) -> None:
        """
        Login to get CSRF token and cookies.
        """
        data = {"username": self.username, "password": self.password, "provider": "db", }

        #soup = BeautifulSoup(response.text, "html.parser")
        # set cookies
        response = self.session.post(self.baseurl / "api/v1/security/login", json=data)
        print(response.json())
        self.access_token = response.json().get('access_token')
        self.refresh_token = response.json().get('refresh_token')
        self.session.headers["Authorization"] = f"Bearer {self.access_token}"
        response = self.session.get(self.baseurl / "api/v1/security/csrf_token")
        input_ = response.json()
        csrf_token = input_["result"] if input_ else None
        if csrf_token:
            self.session.headers["X-CSRFToken"] = csrf_token
            #data["csrf_token"] = csrf_token
            self.csrf_token = csrf_token
