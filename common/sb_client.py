import os
from supabase import create_client


class SupabaseClient:
    """
    A simplified proxy for the real supabase client
    """

    def __init__(self):
        sb_url: str = os.environ.get("SUPABASE_URL")
        sb_key: str = os.environ.get("SUPABASE_KEY")
        self.client = create_client(sb_url, sb_key)
        self.sign_in()

    def sign_in(self):
        sb_email: str = os.environ.get("SUPABASE_EMAIL")
        sb_password: str = os.environ.get("SUPABASE_PASSWORD")
        self.client.auth.sign_in_with_password(
            {"email": sb_email, "password": sb_password}
        )

    def user_id(self):
        res = self.client.auth.get_user()
        return res.user.id

    def sign_out(self):
        self.client.auth.sign_out()

    def table(self, table_name):
        return self.client.table(table_name)

    def invoke_function(self, function_name, invoke_options=None):
        return self.client.functions.invoke(function_name, invoke_options)
