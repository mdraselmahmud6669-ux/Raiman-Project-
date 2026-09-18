# Raiman-Project

Raiman-Project is an email-based multi-agent system designed for automated task execution.

## Workflow
The project operates through the following steps:

1. **Intake**: Command intake.
2. **Parse**: Parse the command.
3. **Discovery**: Discovery agent execution.
4. **Production**: Production agent execution.
5. **Approval**: Approval gate.
6. **Distribute**: Distribution agent execution.
7. **Report**: Final report generation.

## Setup
Install the necessary libraries:
```bash
pip install -r requirements.txthere
​Set the environment variables correctly, especially the email settings for 'DIRECTOR_EMAIL', 'DISCOVERY_AGENT_EMAIL', and 'PRODUCTION_AGENT_EMAIL'.
