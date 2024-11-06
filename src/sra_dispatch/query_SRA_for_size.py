import gc
from datetime import date, datetime, timedelta

import pandas as pd
from loguru import logger as lager
from pysradb.search import SraSearch


def generate_SRR_size_df(configs: dict) -> pd.DataFrame:
    """
    Build a query into SRA using config file specified dates. The dates are called start and end and are from a chronological
    perspective. Start means the day further back in time and end is more recent in time. All of the values handled by
    this function are the query and dates and a check of minimum size of submissions to enable balancing. End date set
    to today will have one day subtracted to prevent downloading midday and missing later submissions.

    Output of this function will be a pandas dataframe with two columns. The SRR/ERR run name along with
    the size of the sequencing files that we will download later. This dataframe will then be used in
    the next function called balance_nodes will dynamically balance the distributed computing requests.
    """
    # Handle "today" and valid date strings for the start date
    if configs["dates"]["start"] == "today":
        start_date = date.today().strftime("%d-%m-%Y")
    else:
        # Try parsing the start date
        try:
            start_date = datetime.strptime(
                configs["dates"]["start"],
                "%d-%m-%Y",
            ).strftime("%d-%m-%Y")
        except ValueError as e:
            raise ValueError(
                f"Error parsing start date: {e}. Ensure it follows the format 'dd-mm-yyyy'.",
            )

    # Handle "today" for the end date
    if configs["dates"]["end"] == "today":
        end_date = (date.today() - timedelta(days=1)).strftime("%d-%m-%Y")
    else:
        try:
            end_date = datetime.strptime(configs["dates"]["end"], "%d-%m-%Y").strftime(
                "%d-%m-%Y",
            )
        except ValueError as e:
            raise ValueError(
                f"Error parsing end date: {e}. Ensure it follows the format 'dd-mm-yyyy'.",
            )

    # Assert that both dates are valid and make logical sense (start date <= end date)
    assert datetime.strptime(start_date, "%d-%m-%Y") <= datetime.strptime(
        end_date,
        "%d-%m-%Y",
    ), f"Start date {start_date} cannot be after end date {end_date}."

    lager.info(f"Start date: {start_date}, End date: {end_date}")

    # Generate the query using corrected f-string syntax (single quotes inside f-string)
    # FIXME rename this
    bioproject_query = SraSearch(
        query=f'{configs["query"]["keyword1"]} {configs["query"]["keyword2"]}',
        publication_date=f"{start_date}:{end_date}",
        verbosity=2,
        return_max=100000,
    )

    # Execute the query
    bioproject_query.search()

    # We can see the SRX numbers of newly published bioprojects AKA last entries and only run those!!!
    bioproject_query.get_df().to_csv("ww_sars-cov-2_meta.csv", sep="\t")

    # clean up some memory
    gc.collect()

    SRR_with_sizes = bioproject_query.get_df()[["run_1_accession", "run_1_size"]]

    # Check step to ensure that load balancing makes sense, exits early if condition from config.json
    # file is met
    if (
        len(SRR_with_sizes)
        < configs["process_configs"]["minimum_submissions_for_balancing"]
    ):
        with open(f"{start_date}_{end_date}", "w") as f:
            # File is built that contains a sequential list of SRR's that can be used
            f.write("\n".join(set(SRR_with_sizes["run_1_accession"].tolist())))
        exit(f"Too few SRR submissions to balance, consider running locally \
             \n File: {start_date}_{end_date} was built and contains all run accessions\n\
            for use with modules/sra_cryptic_loop.sh -s {start_date}_{end_date}")

    return SRR_with_sizes
