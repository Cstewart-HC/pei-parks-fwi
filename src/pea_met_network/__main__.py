"""Allow running the pipeline as `python -m pea_met_network`."""

import os
import sys


def main():
    if os.environ.get("PEA_CUDF", "0") == "1":
        try:
            import cudf.pandas
            print("cuDF.pandas active", file=sys.stderr)
        except ImportError:
            print("cuDF not available", file=sys.stderr)
    from pea_met_network.cleaning import main as pipeline_main
    pipeline_main()


if __name__ == "__main__":
    main()
