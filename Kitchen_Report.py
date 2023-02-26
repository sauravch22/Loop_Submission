import API

if __name__ == '__main__':
    flag = True
    print("Loop Kitchen See your kitchen heartbeat\n")
    while flag:
        print("1. Generate kitchen report ")
        print("2. Generate your kitchen report")
        print("3. View your report")
        print("4. Exit kitchen")
        io = input()
        if io == '1':
            ReportId = API.Report_init()
            print("Generating Report for report id ",ReportId)
        elif io == '2':
            Store_Id = input("Enter the Store ID")
            API.generate_report_for_particular(Store_Id)
        elif io == '3':
            ReportId = input("Enter Report Id\n")
            API.Report_Display(ReportId)
        else:
            flag = API.exit_kitchen()