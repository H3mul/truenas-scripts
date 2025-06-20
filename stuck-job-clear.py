from datetime import datetime, timedelta, timezone
from truenas_api_client import Client
import argparse

# From issue response:
# https://github.com/democratic-csi/democratic-csi/issues/332#issuecomment-2212680497

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--method', help="Job method to filter by, eg 'replication.run_onetime'")
    parser.add_argument('-s', '--since', type=int, default=0, help="Only clear jobs that are older than x min")
    parser.add_argument('--dry-run', action='store_true', help="Only list current jobs, dont close")
    args = parser.parse_args()

    # Initialize Middleware Client
    middleware = Client()

    try:
        # Get current time as a timezone-aware datetime in UTC
        current_time = datetime.now(timezone.utc)
        filter_time = current_time - timedelta(minutes=args.since)

        print("Cleaning up old jobs...")

        if args.dry_run:
            print(' - Dry run')

        if args.since > 0:
            print(f' - older than {args.since} minutes')

        if args.method:
            print(f' - matching method `{args.method}`')

        filters = [
            ['state', '=', 'RUNNING'],
            ['progress.description', '=', '']
        ]

        if args.method:
            filters.append(['method', '=', args.method])

        jobs = middleware.call('core.get_jobs', filters)
        print(f'[+] Found {len(jobs)} total jobs')

        for job in jobs:
            start_time = job['time_started']

            # Check if the job started more than arg minutes ago
            if args.since == 0 or start_time <= filter_time:
                job_id = job['id']
                start_time_formatted = start_time.strftime("%d %B %Y %H:%M:%S")
                running_time = current_time - start_time
                running_time_minutes = running_time.total_seconds() // 60.0

                print(f'Aborting jobId: {job_id}')
                print(f' - Started: {start_time_formatted}')
                print(f' - Runtime: {running_time_minutes}m')

                if not args.dry_run:
                    # Abort the job
                    abort_result = middleware.call('core.job_abort', job_id)
                    if abort_result is None:
                        print(f"-- Success")
                    else:
                        print(f"-- Failed")

    except Exception as e:
        print(f"[-] An error occurred: {e}")
    finally:
        # Close the middleware client connection
        middleware.close()

if __name__ == "__main__":
    main()
