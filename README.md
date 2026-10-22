1. Create local folder
   C:\Projects\payer_01_extracts

2. git init

3. Create minimal project structure
   README.md
   .gitignore
   databricks.yml
   src/
   resources/
   etc.

4. Make initial commit on main

5. Create empty GitHub repository
   payer_01_extracts

6. Connect local Git to GitHub
   git remote add origin <GitHub repository URL>

7. Push main
   git push -u origin main

8. Create develop and release

9. Push develop and release

10. Configure GitHub rulesets / branch protection

11. Switch back to develop

    1. Python virtual environment
    2. Databricks CLI
    3. Databricks CLI profile/authentication
    4. Databricks Connect
    5. Test VS Code → Databricks connection
    6. Then create feature/hello-databricks
    7. databricks.yml
    8. hello_databricks.py
    9. hello_job.yml


12. Create feature branch
   feature/hello-databricks

13. Build
   src/hello_databricks.py
   resources/hello_job.yml

14. Test from VS Code / Databricks

15. Push feature branch

16. Create PR
   feature/hello-databricks → develop


#******************************************************************************************

PHASE 1 — New real repository
-----------------------------
Create new GitHub repository
Clone/open it in VS Code
Create develop / release / main
Protect the branches

PHASE 2 — Databricks foundation
-------------------------------
Create databricks.yml
Create DEV / TEST / PROD targets
Create hello_databricks.py
Create hello_job.yml

Test:
VS Code → Databricks
Databricks authentication
DAB validate
DAB deploy
DAB run
Catalog access

PHASE 3 — CI/CD from VS Code
----------------------------
This time write:
  databricks-ci.yml
  databricks-cd.yml

directly in VS Code.

No building workflows in GitHub UI.

Push through:
feature → develop → release → main

Prove:
DEV → TEST → PROD

PHASE 4 — Actual extracts
-------------------------
Member Extract
Claim Extract
Practitioner Extract
Supplier Extract

PHASE 5 — Improve engineering
-----------------------------
Automated unit tests
CI test execution
Configuration management
Service principal / workload identity
Better deployment controls