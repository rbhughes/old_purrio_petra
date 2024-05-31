from common.typeish import Message, validate_message
from common.util import hostname


class Messenger:
    def __init__(self, sb_client):
        self.sb_client = sb_client
        self.user_id = sb_client.user_id()

    # def send(self, message):
    #     base = {"user_id": self.user_id, "worker": hostname()}
    #     msg: Message = validate_message({**base, **message})
    #     try:
    #         self.sb_client.table("message").insert(msg.to_dict()).execute()
    #     except Exception as e:
    #         print("!!!!!!!!!!!!!!!!!!!!!!")
    #         print(e)
    #         print("!!!!!!!!!!!!!!!!!!!!!!")

    def send(self, directive, repo_id=None, data=None, workflow=None):
        message = {
            "user_id": self.user_id,
            "worker": hostname(),
            "directive": directive,
            "repo_id": repo_id,
            "data": data,
            "workflow": workflow,
        }
        msg: Message = validate_message(message)
        try:
            self.sb_client.table("message").insert(msg.to_dict()).execute()
        except Exception as e:
            print(e)
