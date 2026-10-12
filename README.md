# daily-record-lab

A bounded, manually started integrity observation of public GMS ranking data.

## Scope

One standard Ubuntu runner, one HTTPS connection, and 0.4 requests per second. The test reads the first 100 non-overlapping Bishop pages across both world types, then rechecks the first and last pages. Maximum: 102 GET requests. It does not collect the full class or all LV275+ characters.

The program stops immediately on an HTTP rejection, network failure, empty or short page, unexpected class, duplicate character, changed totalCount, or changed anchor. It does not retry, rotate identities, or automatically schedule further runs.

## Run

Open Actions, select Single-runner integrity observation, and choose Run workflow. Running probe_job_pages.py without --execute only prints the plan.

## Acceptance

Inspect report.json in the single-runner-observation artifact: status complete_sample, 100 successful scan pages, 1000 unique characters, zero duplicate rows, and matching anchors. A green workflow alone is not evidence of reliable full collection.

Artifacts expire after three days. They contain public ranking rows, image URLs (no downloaded image files), and request timing/status records. The socket local address may be a private cloud address; it is not a measured public egress IP. No credentials or NAS database are required.

This experiment measures a small sample. It does not establish long-term stability or explain the provider's rate-limiting rules.
