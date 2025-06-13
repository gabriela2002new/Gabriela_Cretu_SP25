PASSING_GRADE = 8


class Trainee:
    visited_lectures=0
    done_home_tasks=0
    missed_lectures=0
    missed_home_tasks=0
    mark=0
    def __init__(self, name, surname):
        self.name=name
        self.surname=surname

    def visit_lecture(self):
        self.visited_lectures+=1
        self._add_points(1)


    def do_homework(self):
        self.done_home_tasks+=2
        self._add_points(2)

    def miss_lecture(self):
        self.missed_lectures-=1
        self._subtract_points(1)

    def miss_homework(self):
        self.missed_home_tasks-=2
        self._subtract_points(2)

    def _add_points(self, points: int):
        self.mark=self.mark+points
        if self.mark>10:
            self.mark=10

    def _subtract_points(self, points):
        self.mark=self.mark-points
        if self.mark<0:
            self.mark=0

    def is_passed(self):
        if self.mark>=8:
            print("Good job!")
        else:
            print("You need to",8-self.mark ,"points. Try to do your best!")

    def __str__(self):
        status = (
            f"Trainee {self.name.title()} {self.surname.title()}:\n"
            f"done homework {self.done_home_tasks} points;\n"
            f"missed homework {self.missed_home_tasks} points;\n"
            f"visited lectures {self.visited_lectures} points;\n"
            f"missed lectures {self.missed_lectures} points;\n"
            f"current mark {self.mark};\n"
        )
        return status
