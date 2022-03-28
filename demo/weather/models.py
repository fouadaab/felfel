from django.db import models
from django.core.exceptions import ValidationError

# Inspect models:
# python3 manage.py dbshell
# .tables
# .header on
# .mode column
# pragma table_info('table_name');


class Station(models.Model):
    name = models.TextField(primary_key=True)
    longitude = models.FloatField()
    lattitude = models.FloatField()
    grid_i = models.IntegerField()
    grid_j = models.IntegerField()
    grid_height = models.FloatField()


class Unit(models.Model):
    parameter = models.TextField(primary_key=True)
    unit = models.TextField()


class Entry(models.Model):
    station = models.ForeignKey(
        Station,
        on_delete=models.CASCADE,
        related_name='station',
    )
    weather_param = models.ForeignKey(
        Unit,
        on_delete=models.CASCADE,
        related_name='weather_param',
    )
    time = models.DateTimeField()
    member = models.IntegerField()
    leadtime = models.TextField()
    value = models.FloatField(default=-999.0)

    # unique_together with ForeignKey(s) can not be enforced
    # The below method will allow to perform health checks on unique PK
    def validate_unique(self, exclude=None):
        if Entry.objects.filter(
            station_id=self.station,
            weather_param_id=self.weather_param,
            time=self.time,
            member=self.member,
        ).exists():
            raise ValidationError(
                message='This combination of ("station", "weather_param", "time", "member") already exists.',
            )
        models.Model.validate_unique(self, exclude=exclude)