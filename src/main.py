from dotenv import load_dotenv
from connect import connect_to_db
from create_tables import create_tables
from construct_trajs_stops import construct_trajectories_and_stops

def main():
    load_dotenv()
    
    connection = connect_to_db() 
    
    # Create tables Trajectory and Stop
    create_tables(connection)
    
    # Construct Trajectories and Stops from the Points Materialized View 
    construct_trajectories_and_stops(connection)
    
    connection.close()
     
if __name__ == "__main__":
    main()