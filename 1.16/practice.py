import math
def compute():
    money = input("How much money do you have?")
    switchCost = input("How much is a switch?")
    return math.floor(money/switchCost);


# If statements

def age():
    years = input("How old are you?")
    drink = False
    married = False

    if years >= 21:
        drink = True

    if years >= 16:
        married = True

    if drink == True:
        print("Congrats, you can drink on your wedding day")

    if drink == False and married == True:
        print("Sorry, you can get married, but not drink")

    if married == False:
        print("Wow, you are young. You need to grow up")

def main():

    laugh = input("how much money do you have?")
    main()
