import socket
class SFTPConnectError(Exception):
    """"""
    def __init__(self, explanation: str) -> None:
        self.explanation = explanation

    def __str__(self) -> str:
        return "sftp over ssh connect exception; {}".format(self.explanation)

class SFTPConnectMaxRetryError(SFTPConnectError, socket.error): # from socket.error
    """"""

    def __init__(self, max_retries: int) -> None:
        self.max_retries = max_retries
        SFTPConnectError.__init__(self,
                                  "reach max retry: {}".format(self.max_retries))

    
class SFTPconnectNoValidError(SFTPConnectError): # from NoValidConnectionsError
    """"""

    def __init__(self) -> None:
        self.explanation = "multiple connection attempts were made and no families succeeded."
        SFTPConnectError.__init__(self, explanation=self.explanation)

class ListDirLocalError(Exception): # TODO:
    """"""

    pass 

class ListDirRemoteError(Exception): # TODO:
    """"""

    pass

class CollectDownloadTaskError(Exception):
    """"""

    pass 

class DownloadTaskError(Exception):
    """"""

    pass
 
class MYSQLConnectionError(Exception): # TODO:
    """"""

    pass

class GetTaskError(Exception):
    """"""

    pass

class RegisterTaskError(Exception):
    """"""

    pass 

class UnRegisterTaskError(Exception):
    """"""

    pass 

class DeleteTaskError(Exception):
    """"""

    pass 

class LoadTaskError(Exception):
    """"""

    pass

class BBOTaskFailError(Exception):
    """"""

    pass

class ExceptBBOTaskFailError(Exception):
    """"""

    pass 

class SasProgramExecuteError(Exception):
    """"""

    pass


if __name__ == '__main__':

    import paramiko
    from paramiko.ssh_exception import *
    import logging
    logging.basicConfig(filename='test.log',
                        level=logging.ERROR,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        encoding='utf8')
    def connect():
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname='140.115.70.34', port=22, username='chris-pan')
            return client
        except AuthenticationException as e:
            raise SFTPConnectMaxRetryError(3) from e
    
    # try:
    #     connect()
    # except SFTPConnectError as e:
    #     # 在此处可以处理 SFTPConnectMaxRetryError 异常，或者继续向上层抛出
    #     logging.exception("sftp over ssh connect error: ")
    #     raise
    # except Exception as e:
    #     # 其他异常处理
    #     logging.exception("An error occurred: ")
    #     raise

    # print("Continue")
    try:
        try:
            connect()
        except Exception as e:
            raise SFTPConnectError("unknown error") from e
    except: 
        logging.exception("Msg")
        raise
    

    