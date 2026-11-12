# Internship Project - By V Pavithra - Team Leader
# Branch: V-Pavithra
# Project: Internship Daily Task Manager

print("Welcome to Internship Task Manager")
print("Team Leader: V Pavithra")
print("-----------------------------------")

# List to store tasks
tasks = []

# Add tasks for internship
tasks.append("Complete Week 1 Write-up in Infotact")
tasks.append("Push daily code to GitHub")
tasks.append("Create branch for team members")
tasks.append("Merge team code to main branch")
tasks.append("Prepare for Mid Review")

print("Total Tasks Today:", len(tasks))

# Show all tasks one by one
for i, task in enumerate(tasks, start=1):
    print(f"{i}. {task}")

# Mark first task as done
completed = 1
pending = len(tasks) - completed

print("-----------------------------------")
print("Completed Tasks:", completed)
print("Pending Tasks:", pending)

# Simple progress calculation
progress = (completed / len(tasks)) * 100
print(f"Progress: {progress:.1f}%")

# Check if ready for Mid Review
if progress >= 50:
    print("Good progress! Keep going.")
else:
    print("Need more work. Push daily commits.")

# Team members list
team = ["Pavithra - Leader", "Aviral - Member"]
print("-----------------------------------")
print("Team Members:")
for member in team:
    print("-", member)

print("Project updated successfully in V-Pavithra branch")
