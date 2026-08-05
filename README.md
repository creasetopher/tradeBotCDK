# Chris's tradebot.

This is Chris's tradebot. Don't mind the text below, it is simply a template from AWS Python CDK.


## Welcome to your CDK Python project!

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `requirements.txt` file and rerun the `python -m pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

## Market-data end-to-end test

The opt-in end-to-end test verifies the deployed live path from an active
candidate through the Fargate/yfinance worker, Kinesis, and the writer Lambda
to both S3 JSONL and DynamoDB. Deploy the current stack first so it includes
the worker service CloudFormation output, then run the test with AWS
credentials that can read stack outputs and data plus update the ECS service:

```bash
RUN_E2E=1 E2E_STAGE=dev .venv/bin/pytest -m e2e -v
```

Run it while US equities are publishing quotes. `E2E_SYMBOL` (default `AAPL`),
`E2E_STACK_NAME`, `E2E_UNIVERSE_ID`, and `E2E_TIMEOUT_SECONDS` can be overridden.
The test restores the ECS service's original desired count and restores any
pre-existing ActiveCandidatesTable item during cleanup. Ordinary `pytest` runs skip it.
