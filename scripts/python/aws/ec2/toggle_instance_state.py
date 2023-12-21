# if you know an instance is running/stopped, then you can stop/start it
import click
import boto3


@click.command()
@click.option('--name', type=str, help='The name of the EC2 instance we want to change the state of.')
@click.option('--to_state', type=str, help='The state we want our EC2 instance to be in.')

def change_instance_state(name, to_state):
    ec2      = boto3.resource('ec2')
    to_state = to_state.lower()

    for instance in ec2.instances.all():
        for tag in instance.tags:
            if tag['Key'] == 'Name':
                if tag['Value'] == name:
                    state = instance.state['Name']
                    print(f'Instance Name: {name}\nInstance ID: {instance.instance_id}\nState: {state}')
                    if state == to_state:
                        print(f'Instance is already in state "{to_state}", skipping...')
                    else:
                        inp = input(f'Change state to "{to_state}"? (y/n):  ').lower().rstrip()
                        if inp == 'y':
                            if to_state in ('running', 'starting'):
                                instance.start()
                                print('Waiting for the instance to start running...')
                                instance.wait_until_running()
                                print('Success!')
                            elif to_state == 'stopped':
                                instance.stop()
                                print('Success!')
                                return instance.public_ip_address
                        else:
                            print('Instance state change rejected by user.')


if __name__ == '__main__':
    ip = change_instance_state()
