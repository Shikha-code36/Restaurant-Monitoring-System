from app import create_app
import threading
from app.services import refresh_cache

# start the refresh_cache() function in a separate thread
t = threading.Thread(target=refresh_cache)
t.start()

app = create_app()

if __name__ == '__main__':
    app.run(debug=True)
