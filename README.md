### Pre-requisites

Python `virtualenv-3.x` binary should be available on the path.


### Setup

```bash
./prebuild.sh
```

Running export to S3:

```bash
zendesk_to_s3.py calls -a support@blinkhealth.com -t 06is94FOOrtqmf3VVkP2WOPz40y94LbVEUiC1nDF -b s3://blink-dw-data-zendesk/calls
zendesk_to_s3.py tickets -a support@blinkhealth.com -t 06is94FOOrtqmf3VVkP2WOPz40y94LbVEUiC1nDF -b s3://blink-dw-data-zendesk/tickets
```
