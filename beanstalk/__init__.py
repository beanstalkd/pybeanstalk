import job

def daemonize(nchildren=0):
    pass

def main(conn):
    while True:
        job = conn.reserve()
        try:
            job.run()
        except:
            job.release()
        else:
            job.delete()

