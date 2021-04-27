import scraper as sc
import interface as itf

print(itf.welcome)


print("Start scraping? [0] [1]")
choice = sc.choose(int(input("Selection: ")))

print(choice)


