from daily_fetch import main as run_daily_fetch
import traceback


def run(request):
    try:
        request_json = request.get_json(silent=True)
        date = request_json.get("date") if request_json else None
        print(f"Received request for date: {date or 'yesterday'}")
        run_daily_fetch(date)
        return "Daily fetch completed", 200
    except Exception as e:
        print("An error occurred during execution:")
        traceback.print_exc()
        return f"Internal Server Error: {str(e)}", 500
