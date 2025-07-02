from django.urls import path

from . import views

app_name = 'oper'

urlpatterns = [
    path('', views.home, name='home'),
    path('api/status', views.api_status, name='api_status'),
    path('storage/connectivity', views.test_rse, name='test_rse'),
]